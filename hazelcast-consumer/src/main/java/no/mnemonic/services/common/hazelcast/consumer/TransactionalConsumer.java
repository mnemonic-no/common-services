package no.mnemonic.services.common.hazelcast.consumer;

import no.mnemonic.services.common.hazelcast.consumer.exception.ConsumerGaveUpException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * @deprecated Use <code>hazelcast5-consumer</code> package instead
 */
@Deprecated
public interface TransactionalConsumer<T> extends Closeable {

  void consume(Collection<T> items) throws IOException, InterruptedException, ConsumerGaveUpException;

}
