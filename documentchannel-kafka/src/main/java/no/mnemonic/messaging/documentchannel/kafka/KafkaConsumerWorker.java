package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static no.mnemonic.commons.utilities.collections.CollectionUtils.isEmpty;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

class KafkaConsumerWorker<T> implements Runnable {

  private static final int CONSUMER_POLL_TIMEOUT_MILLIS = 1000;
  private static final Logger LOGGER = Logging.getLogger(KafkaConsumerWorker.class);

  private final KafkaDocumentSource<T> consumer;
  private final DocumentChannelListener<T> listener;
  private final KafkaDocumentSource.ConsumerCallbackInterface consumerInterface;
  private final AtomicBoolean cancelled = new AtomicBoolean();

  KafkaConsumerWorker(
          KafkaDocumentSource<T> consumer,
          DocumentChannelListener<T> listener,
          KafkaDocumentSource.ConsumerCallbackInterface consumerInterface
  ) {
    this.consumer = consumer;
    this.listener = listener;
    this.consumerInterface = consumerInterface;
  }

  public void cancel() {
    cancelled.set(true);
  }

  @Override
  public void run() {
    try {
      consumerInterface.consumerRunning(true);
      while (!cancelled.get() && !consumerInterface.isShutdown()) {
        consumeBatch();
      }
    } catch (Exception e) {
      LOGGER.error(e, "Kafka ConsumerWorker failed unrecoverable, stopping ConsumerWorker thread");
    } finally {
      LOGGER.info("Close Kafka Consumer Worker");
      consumerInterface.consumerRunning(false);
      consumer.close();
    }
  }

  private void consumeBatch() {
    KafkaDocumentBatch<T> records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MILLIS));
    try {
      for (KafkaDocument<T> rec : records.getKafkaDocuments()) {
        consumerInterface.register(rec.getRecord());
        if (listener instanceof KafkaDocumentChannelListener) {
          ((KafkaDocumentChannelListener<T>) listener).documentReceived(new KafkaDocument<>(rec.getRecord(), consumerInterface.cursor()));
        } else {
          listener.documentReceived(rec.getDocument());
        }
      }
      //when all documents in batch is consumed, send commit to consumer
      if (!isEmpty(records.getKafkaDocuments())) {
        records.acknowledge();
      }
    } catch (Exception e) {
      // Error when processing events
      consumerInterface.processError(e);
      tryTo(records::reject, ex->LOGGER.error(ex, "Error rejecting batch"));
      // then continue, cause next poll with new offsets
    }
  }

}
