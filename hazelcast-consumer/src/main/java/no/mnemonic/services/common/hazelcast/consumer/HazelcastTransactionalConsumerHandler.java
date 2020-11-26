package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.*;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.services.common.hazelcast.consumer.exception.ConsumerGaveUpException;

import javax.inject.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;

public class HazelcastTransactionalConsumerHandler<T> implements LifecycleAspect, MetricAspect {

  private static final Logger LOG = Logging.getLogger(HazelcastTransactionalConsumerHandler.class);

  private static final int DEFAULT_WORKERS_COUNT = 1;
  private static final int DEFAULT_BULK_SIZE = 100;
  private static final long DEFAULT_HAZELCAST_QUEUE_POLL_TIMEOUT_SEC = 1;
  private static final long DEFAULT_HZ_TRANSACTION_TIMEOUT_SEC = TimeUnit.MINUTES.toSeconds(2L);  // same default as from HZ TransactionOptions
  private static final boolean DEFAULT_HAZELCAST_KEEP_THREAD_ALIVE_ON_EXCEPTION = false;

  @Dependency
  private final HazelcastInstance hazelcastInstance;
  private final String hazelcastQueueName;
  @Dependency
  private final Provider<TransactionalConsumer<T>> consumerProvider;

  private int workerCount = DEFAULT_WORKERS_COUNT;
  private int bulkSize = DEFAULT_BULK_SIZE;
  private long hazelcastQueuePollTimeoutSec = DEFAULT_HAZELCAST_QUEUE_POLL_TIMEOUT_SEC;
  private long hazelcastTransactionTimeoutSec = DEFAULT_HZ_TRANSACTION_TIMEOUT_SEC;
  private boolean keepThreadAliveOnException = DEFAULT_HAZELCAST_KEEP_THREAD_ALIVE_ON_EXCEPTION;

  private final AtomicInteger runningWorkers = new AtomicInteger();
  private final LongAdder queuePollTimeoutCount = new LongAdder();
  private final PerformanceMonitor queuePoll = new PerformanceMonitor(TimeUnit.SECONDS, 10, 1);
  private final PerformanceMonitor bulkSubmit = new PerformanceMonitor(TimeUnit.SECONDS, 10, 1);
  private final PerformanceMonitor workerExecution = new PerformanceMonitor(TimeUnit.SECONDS, 10, 1);
  private final LongAdder itemSubmitCount = new LongAdder();
  private final LongAdder skipEmptyBulk = new LongAdder();

  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<ExecutorService> workerPool = new AtomicReference<>();
  private final Collection<WorkerLifecycleListener> workerLifecycleListeners = new ArrayList<>();

  public HazelcastTransactionalConsumerHandler(
          HazelcastInstance hazelcastInstance,
          String hazelcastQueueName,
          Provider<TransactionalConsumer<T>> consumerProvider
  ) {
    if (hazelcastInstance == null) throw new IllegalArgumentException("hazelcastInstance not provided");
    if (hazelcastQueueName == null) throw new IllegalArgumentException("hazelcastQueueName not provided");
    if (consumerProvider == null) throw new IllegalArgumentException("consumerProvider not provided");
    this.hazelcastInstance = hazelcastInstance;
    this.hazelcastQueueName = hazelcastQueueName;
    this.consumerProvider = consumerProvider;
  }

  @Override
  public void startComponent() {
    LOG.info("Start consumer for queue %s with %d concurrent sync workers", hazelcastQueueName, workerCount);

    if (bulkSize * hazelcastQueuePollTimeoutSec >= hazelcastTransactionTimeoutSec) {
      // Prevent HZ transaction time out when polling from HZ for bulkSize amount of data,
      // so that the time waiting for poll would not exceed the HZ transaction time out.
      throw new IllegalStateException(String.format("bulkSize (%d) * HZ poll timeout (%ds) must be less than HZ transaction timeout (%ds).",
              bulkSize, hazelcastQueuePollTimeoutSec, hazelcastTransactionTimeoutSec));
    }

    ensureWorkerPool();
    for (int i = 0; i < workerCount; i++) {
      workerPool.get().submit(this::runWorker);
    }
    running.set(true);
  }

  @Override
  public void stopComponent() {
    LOG.info("Stop %s", getClass().getSimpleName());
    running.set(false);
    workerPool.updateAndGet(p -> {
      try {
        p.shutdown();
        p.awaitTermination(10, TimeUnit.SECONDS);
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted when shutdown thread pool", e);
      }
    });
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsData metrics = new MetricsData();
    metrics.addData("alive", running.get() && runningWorkers.get() > 0 ? 1 : 0);
    metrics.addData("workers.all.alive", running.get() && runningWorkers.get() == workerCount ? 1 : 0);
    metrics.addData("workers.running.count", runningWorkers.get());

    metrics.addData("queue.poll.count", queuePoll.getTotalInvocations());
    metrics.addData("queue.poll.time.spent", queuePoll.getTotalTimeSpent());
    metrics.addData("queue.poll.timeout.count", queuePollTimeoutCount);
    metrics.addData("item.submit.count", itemSubmitCount.longValue());
    metrics.addData("bulk.skip.count", skipEmptyBulk.longValue());
    metrics.addData("bulk.submit.count", bulkSubmit.getTotalInvocations());
    metrics.addData("bulk.submit.time.spent", bulkSubmit.getTotalTimeSpent());
    metrics.addData("worker.execute.count", workerExecution.getTotalInvocations());
    metrics.addData("worker.execute.time.spent", workerExecution.getTotalTimeSpent());

    return metrics;
  }

  private void runWorker() {
    LOG.info("%s start", Thread.currentThread().getName());
    runningWorkers.incrementAndGet();

    try (TransactionalConsumer<T> consumer = consumerProvider.get()) {
      while (!ifNotNull(workerPool.get(), ExecutorService::isShutdown, true)) {
        consumeNextBatch(consumer);
      }
    } catch (Exception e) {
      // Catch all exceptions that fail the thread
      LOG.error(e, "%s failed and stop that caused by: %s", Thread.currentThread().getName(), e.getMessage());
    } finally {
      LOG.info("%s finished", Thread.currentThread().getName());
      runningWorkers.decrementAndGet();
      workerLifecycleListeners.forEach(l -> l.workerStopped(Thread.currentThread()));
    }
  }

  private void ensureWorkerPool() {
    workerPool.updateAndGet(p -> {
      if (p != null) return p;

      AtomicLong threadsCount = new AtomicLong(0);
      return Executors.newFixedThreadPool(workerCount, runnable -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName(String.format("%s-Consumer-%d", hazelcastQueueName, threadsCount.getAndIncrement()));
        thread.setUncaughtExceptionHandler((t, e) -> LOG.error(e, "Uncaught exception from thread %s: %s", t.getName(), e.getMessage()));
        return thread;
      });
    });
  }

  private void consumeNextBatch(TransactionalConsumer<T> consumer) throws InterruptedException, IOException, ConsumerGaveUpException {
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
        return;
      }

      // 2. submit items to downstream consumer
      try (TimerContext ignored2 = TimerContext.timerMillis(bulkSubmit::invoked)) {
        consumer.consume(items);
      }

      transactionContext.commitTransaction();
      itemSubmitCount.add(items.size());
    } catch (ConsumerGaveUpException e) {
      transactionContext.rollbackTransaction(); // make sure rollback to let other threads handle the data
      throw e;
    } catch (Exception e) {
      transactionContext.rollbackTransaction(); // make sure rollback to let other threads handle the data
      if (!keepThreadAliveOnException) {
        throw e;  // exception happens which leads thread to stop
      }
      LOG.error(e, "An exception was thrown when consuming batch");
    }
  }

  private List<T> pollItemsFromHazelcastQueue(TransactionalQueue<T> queue) throws InterruptedException {
    List<T> items = new ArrayList<>();
    for (int i = 0; i < bulkSize; i++) {
      if (ifNotNull(workerPool.get(), ExecutorService::isShutdown, true)) {
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

  public HazelcastTransactionalConsumerHandler<T> setWorkerCount(int workerCount) {
    this.workerCount = workerCount;
    return this;
  }

  public HazelcastTransactionalConsumerHandler<T> setBulkSize(int bulkSize) {
    this.bulkSize = bulkSize;
    return this;
  }

  public HazelcastTransactionalConsumerHandler<T> setHazelcastQueuePollTimeoutSec(long hazelcastQueuePollTimeoutSec) {
    this.hazelcastQueuePollTimeoutSec = hazelcastQueuePollTimeoutSec;
    return this;
  }

  public HazelcastTransactionalConsumerHandler<T> setHazelcastTransactionTimeoutSec(long hazelcastTransactionTimeoutSec) {
    this.hazelcastTransactionTimeoutSec = hazelcastTransactionTimeoutSec;
    return this;
  }

  public HazelcastTransactionalConsumerHandler<T> setKeepThreadAliveOnException(boolean keepThreadAliveOnException) {
    this.keepThreadAliveOnException = keepThreadAliveOnException;
    return this;
  }

  //only for testing
  HazelcastTransactionalConsumerHandler<T> setWorkerPool(ExecutorService workerPool) {
    this.workerPool.set(workerPool);
    return this;
  }

  HazelcastTransactionalConsumerHandler<T> addWorkerLifecycleListener(WorkerLifecycleListener l) {
    this.workerLifecycleListeners.add(l);
    return this;
  }

  interface WorkerLifecycleListener {
    void workerStopped(Thread thread);
  }
}
