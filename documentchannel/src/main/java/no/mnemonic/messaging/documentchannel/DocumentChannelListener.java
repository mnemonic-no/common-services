package no.mnemonic.messaging.documentchannel;

/**
 * A listener interface for incoming documents
 *
 * @param <T> document type
 */
public interface DocumentChannelListener<T> {

  /**
   * Invoked for every incoming document
   * @param document the incoming document
   */
  void documentReceived(T document);

}
