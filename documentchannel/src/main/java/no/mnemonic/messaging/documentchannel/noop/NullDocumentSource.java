package no.mnemonic.messaging.documentchannel.noop;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Empty implementation of a DocumentSource, which does absolutely nothing.
 * However, all methods are implemented in a null-safe way.
 *
 * @param <T> the channel document type
 */
public class NullDocumentSource<T> implements DocumentSource<T> {
  private static final Logger LOGGER = Logging.getLogger(NullDocumentSource.class);

  @Override
  public DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> documentChannelListener) {
    return () -> {
      //no nothing
    };
  }

  @Override
  public DocumentBatch<T> poll(Duration duration) {
    // Block the thread for the provided duration in order to prevent underlying threads
    // from spinning endless while-loops, consuming resources for no reason
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      LOGGER.info(e, "Caught InterruptedException in NullDocumentSource");
      Thread.currentThread().interrupt();
    }

    return new DocumentBatch<T>() {
      @Override
      public Collection<T> getDocuments() {
        return new ArrayList<>();
      }

      @Override
      public void acknowledge() {
        //do nothing
      }

      @Override
      public void reject() {
        //do nothing
      }
    };
  }

  @Override
  public void close() {
    //do nothing
  }
}
