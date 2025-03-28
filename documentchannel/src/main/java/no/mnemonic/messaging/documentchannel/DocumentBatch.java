package no.mnemonic.messaging.documentchannel;

import java.util.Collection;
import java.util.Iterator;

/**
 * A batch of documents, returned by {@link DocumentSource#poll}
 *
 * @param <T> document type
 */
public interface DocumentBatch<T> extends Iterable<T> {

  /**
   * @return documents in batch
   */
  Collection<T> getDocuments();

  /**
   * Acknowledge that this batch is handled.
   * The underlying DocumentSource should commit this batch if the source has transactional properties.
   */
  void acknowledge();

  /**
   * Reject document batch
   * The underlying DocumentSource should roll back this batch if the source has transactional properties.
   * This means that the entire batch should be redelivered later (depending on implementation).
   */
  void reject();

  @Override
  default Iterator<T> iterator() {
    return getDocuments().iterator();
  }
}
