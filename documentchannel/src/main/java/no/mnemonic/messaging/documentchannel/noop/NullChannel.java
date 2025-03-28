package no.mnemonic.messaging.documentchannel.noop;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.documentchannel.DocumentChannel;

/**
 * Empty implementation of a DocumentChannel, which does absolutely nothing.
 * However, all methods are implemented in a null-safe way.
 *
 * @param <T> the channel document type
 */
public class NullChannel<T> implements DocumentChannel<T> {

  private static final Logger LOGGER = Logging.getLogger(NullChannel.class);

  @Override
  public void sendDocument(T document) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("sendDocument(document) invoked");
    }
  }

  @Override
  public <K> void sendDocument(T document, K documentKey, DocumentCallback<K> callback) {
    if (LOGGER.isDebug()) {
      LOGGER.debug("sendDocument(document, documentKey, callback)");
    }
  }

  @Override
  public void flush() {
    if (LOGGER.isDebug()) {
      LOGGER.debug("flush() invoked");
    }
  }
}
