package no.mnemonic.messaging.documentchannel;

/**
 * A writer interface to put documents to.
 *
 * @param <T> document type
 */
public interface DocumentChannel<T> {

  /**
   * Submit a new document. The document will be delivered to any configured document channel subscribers
   * for this channel.
   *
   * @param document the document to send
   */
  void sendDocument(T document);

  /**
   * Same as {@link #sendDocument(Object)}. Will invoke the DocumentCallback when the document has been accepted by the channel.
   * Use this method when configuring asyncronous flushing of documents, to let the channel notify the client when the document
   * is considered accepted.
   *
   * @param document the document to send
   * @param documentKey the key to pass to the document callback when this particular document has been accepted
   * @param callback the callback to invoke when channel has accepted the document
   */
  <K> void sendDocument(T document, K documentKey, DocumentCallback<K> callback);

  /**
   * Flush any documents pending in this channel.
   */
  void flush();

  interface DocumentCallback<K> {
    /**
     * Invoked when document channel has accepted the associated message
     * @param key the key passed to {@link #sendDocument(Object, Object, DocumentCallback)}
     */
    void documentAccepted(K key);

    /**
     * Invoked when document channel reports an error delivering this document
     * @param key the key passed to {@link #sendDocument(Object, Object, DocumentCallback)}
     */
    void channelError(K key, Exception exception);
  }

}
