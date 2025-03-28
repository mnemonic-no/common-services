package no.mnemonic.messaging.documentchannel;

/**
 * A configured document channel which represents a session to the channel
 *
 * @param <T> document type
 */
public interface DocumentDestination<T> extends AutoCloseable {

  /**
   * Fetch channel to write to
   * @return the channel to write to
   */
  DocumentChannel<T> getDocumentChannel();

  /**
   * Close this channel
   */
  @Override
  void close();

}
