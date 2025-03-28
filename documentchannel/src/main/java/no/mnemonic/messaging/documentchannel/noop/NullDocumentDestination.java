package no.mnemonic.messaging.documentchannel.noop;

import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentDestination;

/**
 * Empty implementation of a DocumentDestination, which does absolutely nothing.
 * However, all methods are implemented in a null-safe way.
 *
 * @param <T> the channel document type
 */
public class NullDocumentDestination<T> implements DocumentDestination<T> {
  @Override
  public DocumentChannel<T> getDocumentChannel() {
    return new NullChannel<>();
  }

  @Override
  public void close() {
    //do nothing
  }
}
