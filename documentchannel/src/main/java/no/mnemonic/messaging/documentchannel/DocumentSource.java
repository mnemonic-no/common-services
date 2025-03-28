package no.mnemonic.messaging.documentchannel;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A configured document source, which represents a session for this channel.
 * A client can subscribe to this channel by creating a document channel subscription
 * using {@link #createDocumentSubscription(DocumentChannelListener)}
 *
 * @param <T> document type
 */
public interface DocumentSource<T> extends AutoCloseable {

  /**
   * Create a subscription, which will submit incoming documents to the listener
   *
   * @param listener listener to send documents to
   * @return a subscription object, which handles the subscription state
   */
  DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> listener);

  /**
   * Polling the document source for documents. The size of the returned collection
   * is determined by the implementation.
   *
   * @param duration maximum duration to wait
   * @return a collection of documents, or null if no was available in that time
   */
  DocumentBatch<T> poll(Duration duration);

  /**
   * @deprecated Use {@link #poll(Duration)} instead
   */
  @Deprecated
  default DocumentBatch<T> poll(long duration, TimeUnit timeUnit) {
    return poll(Duration.ofMillis(timeUnit.toMillis(duration)));
  }

  /**
   * Close this source session
   */
  @Override
  void close();

}
