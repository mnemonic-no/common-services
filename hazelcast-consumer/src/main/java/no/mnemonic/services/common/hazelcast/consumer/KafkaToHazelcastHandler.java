package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentSource;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

/**
 * This component is consuming from Kafka and then offer to Hazelcast queue (distributed bounded blocking queue),
 * if Hazelcast queue has no space it will block the Kafka consumer thread until there is free space or timeout.
 * <p>
 * no.mnemonic.messaging.documentchannel.kafka.KafkaDocumentSource creates separate thread when handler is subscribed,
 * and the thread will be assigned with one to multiple partitions from Kafka.
 * The thread would consume messages from Kafka in batch, and commit directly after the batch is loaded,
 * which means the offset is moved forward in Kafka.
 * Hazelcast queue is used to store the messages from Kafka to guarantee that the messages won't be lost due to e.g. current server goes down,
 * so that the Hazelcast queue consumer can poll from it.
 */
public class KafkaToHazelcastHandler<T> implements LifecycleAspect, MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(KafkaToHazelcastHandler.class);
  private static final long DEFAULT_HAZELCAST_QUEUE_OFFER_TIMEOUT_SEC = 10;
  private static final long DEFAULT_HZ_TRANSACTION_TIMEOUT_SEC = TimeUnit.MINUTES.toSeconds(2L);
  private static final int CONSUMER_POLL_TIMEOUT_MILLIS = 1000;
  private static final boolean DEFAULT_HAZELCAST_KEEP_THREAD_ALIVE_ON_EXCEPTION = false;

  @Dependency
  private final DocumentSource<T> source;
  private final HazelcastInstance hazelcastInstance;
  private final String hazelcastQueueName;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private long hazelcastQueueOfferTimeoutSec = DEFAULT_HAZELCAST_QUEUE_OFFER_TIMEOUT_SEC;
  private long hazelcastTransactionTimeoutSec = DEFAULT_HZ_TRANSACTION_TIMEOUT_SEC;
  private boolean keepThreadAliveOnException = DEFAULT_HAZELCAST_KEEP_THREAD_ALIVE_ON_EXCEPTION;

  // monitors
  private final AtomicBoolean alive = new AtomicBoolean();
  private final LongAdder documentReceivedCount = new LongAdder();
  private final LongAdder offeredEventsCount = new LongAdder();
  private final LongAdder queueOfferMonitor = new LongAdder();
  private final LongAdder queueOfferFailedCount = new LongAdder();

  public KafkaToHazelcastHandler(
          DocumentSource<T> source,
          HazelcastInstance hazelcastInstance,
          String hazelcastQueueName) {
    this.source = source;
    this.hazelcastInstance = hazelcastInstance;
    this.hazelcastQueueName = hazelcastQueueName;
  }

  @Override
  public void startComponent() {
    LOGGER.info("Start %s subscribes to document source and produce to Hazelcast queue %s",
            getClass().getSimpleName(),
            hazelcastQueueName);
    alive.set(true);
    executorService.submit(new KafkaWorker());
  }

  @Override
  public void stopComponent() {
    LOGGER.info("Stop " + getClass().getSimpleName());
    alive.set(false);
    executorService.shutdown();
    tryTo(()->executorService.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsData metrics = new MetricsData();
    metrics.addData("alive", alive.get() ? 1 : 0);
    metrics.addData("hazelcast.queue.size", hazelcastInstance.getQueue(hazelcastQueueName).size());
    metrics.addData("document.received.count", documentReceivedCount);
    metrics.addData("queue.offer.success.count", offeredEventsCount);
    metrics.addData("queue.offer.failed.count", queueOfferFailedCount);
    metrics.addData("queue.offer.spent.ms", queueOfferMonitor);
    return metrics;
  }

  private void documentReceived(TransactionalQueue<T> queue, T document) throws TransactionTimeoutException {

    documentReceivedCount.increment();

    try (TimerContext ignored = TimerContext.timerMillis(queueOfferMonitor::add)) {

      if (!queue.offer(document, hazelcastQueueOfferTimeoutSec, TimeUnit.SECONDS)) {
        LOGGER.error(String.format("Fail to offer to Hazelcast queue %s after %ds", hazelcastQueueName, hazelcastQueueOfferTimeoutSec));
        queueOfferFailedCount.increment();
        // throw out to stop Kafka consumer thread, which may be retried
        throw new TransactionTimeoutException(String.format("Fail to offer to Hazelcast queue %s after %ds", hazelcastQueueName, hazelcastQueueOfferTimeoutSec));
      }

      // offer success
      offeredEventsCount.increment();

    } catch (InterruptedException e) {
      LOGGER.error(String.format("Interrupted when offer event to Hazelcast queue %s", hazelcastQueueName));
      queueOfferFailedCount.increment();
      // throw out to stop Kafka consumer thread, which may be retried
      Thread.currentThread().interrupt();
      throw new TransactionTimeoutException(String.format("Interrupted when offer event to Hazelcast queue %s", hazelcastQueueName), e);
    }
  }

  public boolean isAlive() {
    return alive.get();
  }

  public KafkaToHazelcastHandler<T> setHazelcastQueueOfferTimeoutSec(long hazelcastQueueOfferTimeoutSec) {
    this.hazelcastQueueOfferTimeoutSec = hazelcastQueueOfferTimeoutSec;
    return this;
  }

  public KafkaToHazelcastHandler<T> setHazelcastTransactionTimeoutSec(long hazelcastTransactionTimeoutSec) {
    this.hazelcastTransactionTimeoutSec = hazelcastTransactionTimeoutSec;
    return this;
  }

  public KafkaToHazelcastHandler<T> setKeepThreadAliveOnException(boolean keepThreadAliveOnException) {
    this.keepThreadAliveOnException = keepThreadAliveOnException;
    return this;
  }

  //for testing use only, to allow testing execution of a single run, without starting the Executor thread
  void runSingle() {
    alive.set(true);
    new KafkaWorker().processBatch();
  }

  private class KafkaWorker implements Runnable {
    @Override
    public void run() {
      while(alive.get()) {
        processBatch();
      }
    }

    private void processBatch() {
      DocumentBatch<T> batch = source.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MILLIS));
      Collection<T> documents = batch.getDocuments();
      if (CollectionUtils.isEmpty(documents)) {
        return;
      }
      TransactionContext transactionContext = hazelcastInstance.newTransactionContext(
              new TransactionOptions()
                      .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE)
                      .setTimeout(hazelcastTransactionTimeoutSec, TimeUnit.SECONDS)
      );
      // Needs to begin transaction in order to get transactional managed queue
      transactionContext.beginTransaction();
      TransactionalQueue<T> transactionalQueue = transactionContext.getQueue(hazelcastQueueName);

      try {

        for (T doc : documents) {
          documentReceived(transactionalQueue, doc);
        }
        transactionContext.commitTransaction();
        batch.acknowledge();

      } catch (TransactionTimeoutException e) {

        transactionContext.rollbackTransaction();
        batch.reject();

        if (!keepThreadAliveOnException) {
          LOGGER.error(e, "Shutting down worker thread after transaction timeout");
          stopComponent();
        }

      }
    }
  }

  private static class TransactionTimeoutException extends Exception {

    public TransactionTimeoutException(String message) {
      super(message);
    }

    public TransactionTimeoutException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
