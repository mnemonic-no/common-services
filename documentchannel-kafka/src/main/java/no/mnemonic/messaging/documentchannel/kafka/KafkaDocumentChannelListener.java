package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.messaging.documentchannel.DocumentChannelListener;

/**
 * An extended listener interface for incoming documents from Kafka.
 * This interface extends the DocumentChannelListener.
 *
 * @param <T> document type
 */
public interface KafkaDocumentChannelListener<T> extends DocumentChannelListener<T> {

  /**
   * Invoked for every incoming document.
   *
   * @param document the incoming document, wrapped into a KafkaDocument transport object
   */
  void documentReceived(KafkaDocument<T> document);

  @Override
  default void documentReceived(T document) {
    throw new UnsupportedOperationException("Not implemented, use documentReceived(KafkaDocument) instead");
  }
}
