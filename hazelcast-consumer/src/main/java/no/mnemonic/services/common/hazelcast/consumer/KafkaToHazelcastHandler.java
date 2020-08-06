package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.*;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;

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
public class KafkaToHazelcastHandler<T> implements DocumentChannelListener<T>, LifecycleAspect, MetricAspect {

  private static final Logger LOG = Logging.getLogger(KafkaToHazelcastHandler.class);
  private static final long DEFAULT_HAZELCAST_QUEUE_OFFER_TIMEOUT_SEC = 10;

  @Dependency
  private final DocumentSource<T> source;

  private final IQueue<T> hazelcastQueue;

  private long hazelcastQueueOfferTimeoutSec = DEFAULT_HAZELCAST_QUEUE_OFFER_TIMEOUT_SEC;

  // monitors
  private final AtomicBoolean alive = new AtomicBoolean();
  private final LongAdder documentReceivedCount = new LongAdder();
  private final LongAdder offeredEventsCount = new LongAdder();
  private final LongAdder queueOfferMonitor = new LongAdder();
  private final LongAdder queueOfferFailedCount = new LongAdder();

  private DocumentChannelSubscription subscription;

  public KafkaToHazelcastHandler(
          DocumentSource<T> source,
          HazelcastInstance hazelcastInstance,
          String hazelcastQueueName) {
    this.source = source;
    this.hazelcastQueue = hazelcastInstance.getQueue(hazelcastQueueName);
  }

  @Override
  public void startComponent() {
    LOG.info("Start %s subscribes to document source and produce to Hazelcast queue %s",
            getClass().getSimpleName(),
            hazelcastQueue.getName());
    subscription = source.createDocumentSubscription(this);
    alive.set(true);
  }

  @Override
  public void stopComponent() {
    LOG.info("Stop " + getClass().getSimpleName());
    ifNotNullDo(subscription, DocumentChannelSubscription::cancel);
    alive.set(false);
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsData metrics = new MetricsData();
    metrics.addData("alive", alive.get() ? 1 : 0);
    metrics.addData("hazelcast.queue.size", hazelcastQueue.size());
    metrics.addData("document.received.count", documentReceivedCount);
    metrics.addData("queue.offer.success.count", offeredEventsCount);
    metrics.addData("queue.offer.failed.count", queueOfferFailedCount);
    metrics.addData("queue.offer.spent.ms", queueOfferMonitor);
    return metrics;
  }

  @Override
  public void documentReceived(T document) {
    documentReceivedCount.increment();
    try (TimerContext ignored = TimerContext.timerMillis(queueOfferMonitor::add)) {
      if (!hazelcastQueue.offer(document, hazelcastQueueOfferTimeoutSec, TimeUnit.SECONDS)) {
        LOG.error(String.format("%s Fail to offer to Hazelcast queue %s after %ds",
                Thread.currentThread().getName(),
                hazelcastQueue.getName(),
                hazelcastQueueOfferTimeoutSec));

        queueOfferFailedCount.increment();
        // throw out to stop Kafka consumer thread, which may be retried
        throw new IllegalStateException(String.format("Fail to offer to Hazelcast queue %s after %ds",
                hazelcastQueue.getName(),
                hazelcastQueueOfferTimeoutSec));
      }

      // offer success
      offeredEventsCount.increment();
    } catch (InterruptedException e) {
      LOG.error(String.format("%s is interrupted when offer event to Hazelcast queue %s",
              Thread.currentThread().getName(),
              hazelcastQueue.getName()));

      queueOfferFailedCount.increment();
      // throw out to stop Kafka consumer thread, which may be retried
      Thread.currentThread().interrupt();
      throw new IllegalStateException(String.format("%s is interrupted when offer event to Hazelcast queue %s",
              Thread.currentThread().getName(),
              hazelcastQueue.getName()), e);
    }
  }

  public KafkaToHazelcastHandler<T> setHazelcastQueueOfferTimeoutSec(long hazelcastQueueOfferTimeoutSec) {
    this.hazelcastQueueOfferTimeoutSec = hazelcastQueueOfferTimeoutSec;
    return this;
  }
}
