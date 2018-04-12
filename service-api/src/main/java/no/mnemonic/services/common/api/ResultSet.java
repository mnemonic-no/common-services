package no.mnemonic.services.common.api;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

  @Override
  default Spliterator<T> spliterator() {
    return Spliterators.spliteratorUnknownSize(
            iterator(),
            Spliterator.ORDERED
    );
  }

  /**
   * @return a stream based on this iterable
   */
  default Stream<T> stream() {
    return StreamSupport.stream(spliterator(), false);
  }
}
