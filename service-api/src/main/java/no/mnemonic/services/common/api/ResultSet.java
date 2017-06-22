package no.mnemonic.services.common.api;

import java.util.Iterator;

public interface ResultSet<T> extends Iterable<T> {

  int getCount();

  int getLimit();

  int getOffset();

  /**
   * A resultset iterator iterates the values of the resultset, which may be lazily loaded
   * as the client iterates them, depending on the implementation.
   * Implementation should wrap any errors during lazy loading into a
   * {@link ResultSetStreamInterruptedException} to signal that the exception is caused by
   * an interrupted result stream.
   *
   * @see Iterable#iterator()
   * @throws ResultSetStreamInterruptedException if initiating the resultset fails
   */
  @Override
  Iterator<T> iterator() throws ResultSetStreamInterruptedException;
}
