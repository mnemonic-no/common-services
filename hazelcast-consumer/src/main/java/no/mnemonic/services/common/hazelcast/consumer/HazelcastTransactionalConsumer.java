package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.PerformanceMonitor;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.services.common.hazelcast.consumer.exception.ConsumerGaveUpException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class HazelcastTransactionalConsumer<T> implements MetricAspect {

  private static final Logger LOG = Logging.getLogger(HazelcastTransactionalConsumer.class);

  private static final int DEFAULT_BULK_SIZE = 100;
  private static final long DEFAULT_HAZELCAST_QUEUE_POLL_TIMEOUT_SEC = 1;
  private static final long DEFAULT_HZ_TRANSACTION_TIMEOUT_SEC = TimeUnit.MINUTES.toSeconds(2L);  // same default as from HZ TransactionOptions
  private static final boolean DEFAULT_HAZELCAST_KEEP_THREAD_ALIVE_ON_EXCEPTION = false;

  private final HazelcastInstance hazelcastInstance;
  private final String hazelcastQueueName;

  private int bulkSize = DEFAULT_BULK_SIZE;
  private long hazelcastQueuePollTimeoutSec = DEFAULT_HAZELCAST_QUEUE_POLL_TIMEOUT_SEC;
  private long hazelcastTransactionTimeoutSec = DEFAULT_HZ_TRANSACTION_TIMEOUT_SEC;
  private boolean keepThreadAliveOnException = DEFAULT_HAZELCAST_KEEP_THREAD_ALIVE_ON_EXCEPTION;

  private final LongAdder queuePollTimeoutCount = new LongAdder();
  private final PerformanceMonitor queuePoll = new PerformanceMonitor(TimeUnit.SECONDS, 10, 1);
  private final PerformanceMonitor bulkSubmit = new PerformanceMonitor(TimeUnit.SECONDS, 10, 1);
  private final PerformanceMonitor workerExecution = new PerformanceMonitor(TimeUnit.SECONDS, 10, 1);
  private final LongAdder itemSubmitCount = new LongAdder();
  private final LongAdder skipEmptyBulk = new LongAdder();
  private final LongAdder bulkFailedCount = new LongAdder();

  public HazelcastTransactionalConsumer(HazelcastInstance hazelcastInstance, String hazelcastQueueName) {
    if (hazelcastInstance == null) throw new IllegalArgumentException("hazelcastInstance not provided");
    if (hazelcastQueueName == null) throw new IllegalArgumentException("hazelcastQueueName not provided");
    this.hazelcastInstance = hazelcastInstance;
    this.hazelcastQueueName = hazelcastQueueName;
  }

  @Override
  public MetricsData getMetrics() throws MetricException {
    return new MetricsData()
            .addData("queue.poll.count", queuePoll.getTotalInvocations())
            .addData("queue.poll.time.spent", queuePoll.getTotalTimeSpent())
            .addData("queue.poll.timeout.count", queuePollTimeoutCount)
            .addData("item.submit.count", itemSubmitCount.longValue())
            .addData("bulk.skip.count", skipEmptyBulk.longValue())
            .addData("bulk.failed.count", bulkFailedCount.longValue())
            .addData("bulk.submit.count", bulkSubmit.getTotalInvocations())
            .addData("bulk.submit.time.spent", bulkSubmit.getTotalTimeSpent())
            .addData("worker.execute.count", workerExecution.getTotalInvocations())
            .addData("worker.execute.time.spent", workerExecution.getTotalTimeSpent());
  }

  /**
   * @param consumer the consumer to handle batches of items to consume
   * @return the number of consumed items
   * @throws InterruptedException if consumer is interrupted waiting for data
   * @throws IOException if consumer fails
   * @throws ConsumerGaveUpException if consumer refuses to retry more times
   */
  public int consumeNextBatch(TransactionalConsumer<T> consumer) throws InterruptedException, IOException, ConsumerGaveUpException {
    TransactionContext transactionContext = hazelcastInstance.newTransactionContext(
            new TransactionOptions()
                    .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
                    .setTimeout(hazelcastTransactionTimeoutSec, TimeUnit.SECONDS)
    );
    // Needs to begin transaction in order to get transactional managed queue
    transactionContext.beginTransaction();
    TransactionalQueue<T> transactionalQueue = transactionContext.getQueue(hazelcastQueueName);

    try (TimerContext ignored = TimerContext.timerMillis(workerExecution::invoked)) {
      // 1. poll from Hazelcast queue
      List<T> items;
      try (TimerContext ignored1 = TimerContext.timerMillis(queuePoll::invoked)) {
        items = pollItemsFromHazelcastQueue(transactionalQueue);
      }

      if (CollectionUtils.isEmpty(items)) {
        transactionContext.commitTransaction();
        skipEmptyBulk.increment();
        // no items found during current round of pollings, continue
        return 0;
      }

      // 2. submit items to downstream consumer
      try (TimerContext ignored2 = TimerContext.timerMillis(bulkSubmit::invoked)) {
        consumer.consume(items);
      }

      transactionContext.commitTransaction();
      itemSubmitCount.add(items.size());
      return items.size();
    } catch (ConsumerGaveUpException e) {
      transactionContext.rollbackTransaction(); // make sure rollback to let other threads handle the data
      throw e;
    } catch (Exception e) {
      transactionContext.rollbackTransaction(); // make sure rollback to let other threads handle the data
      if (!keepThreadAliveOnException) {
        throw e;  // exception happens which leads thread to stop
      }
      bulkFailedCount.increment();
      LOG.error(e, "An exception was thrown when consuming batch");
      return 0;
    }
  }

  protected boolean isShutdown() {
    return false;
  }

  private List<T> pollItemsFromHazelcastQueue(TransactionalQueue<T> queue) throws InterruptedException {
    List<T> items = new ArrayList<>();
    for (int i = 0; i < bulkSize; i++) {
      if (isShutdown()) {
        break;
      }
      T value = queue.poll(hazelcastQueuePollTimeoutSec, TimeUnit.SECONDS);
      if (value == null) {
        queuePollTimeoutCount.increment();
        break; // timeout but no item in the queue
      }
      items.add(value);
    }
    return items;
  }

  public String getHazelcastQueueName() {
    return hazelcastQueueName;
  }

  public int getBulkSize() {
    return bulkSize;
  }

  public HazelcastTransactionalConsumer<T> setBulkSize(int bulkSize) {
    this.bulkSize = bulkSize;
    return this;
  }

  public long getHazelcastQueuePollTimeoutSec() {
    return hazelcastQueuePollTimeoutSec;
  }

  public HazelcastTransactionalConsumer<T> setHazelcastQueuePollTimeoutSec(long hazelcastQueuePollTimeoutSec) {
    this.hazelcastQueuePollTimeoutSec = hazelcastQueuePollTimeoutSec;
    return this;
  }

  public long getHazelcastTransactionTimeoutSec() {
    return hazelcastTransactionTimeoutSec;
  }

  public HazelcastTransactionalConsumer<T> setHazelcastTransactionTimeoutSec(long hazelcastTransactionTimeoutSec) {
    this.hazelcastTransactionTimeoutSec = hazelcastTransactionTimeoutSec;
    return this;
  }

  public boolean isKeepThreadAliveOnException() {
    return keepThreadAliveOnException;
  }

  public HazelcastTransactionalConsumer<T> setKeepThreadAliveOnException(boolean keepThreadAliveOnException) {
    this.keepThreadAliveOnException = keepThreadAliveOnException;
    return this;
  }

}
