package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.transaction.TransactionContext;
import no.mnemonic.services.common.hazelcast.consumer.exception.ConsumerGaveUpException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HazelcastTransactionalConsumerHandlerTest {

  private static final String HAZELCAST_QUEUE_NAME = "My.Queue";

  @Mock
  private HazelcastInstance hazelcastInstance;
  @Mock
  private TransactionContext transactionContext;
  @Mock
  private TransactionalQueue<String> txHazelcastQueue;
  @Mock
  private TransactionalConsumer<String> transactionalConsumer;
  @Mock
  private ExecutorService workerPool;
  @Mock
  private HazelcastTransactionalConsumerHandler.WorkerLifecycleListener workerLifecycleListener;

  private final AtomicReference<Runnable> workerTask = new AtomicReference<>();
  private final AtomicBoolean shutdown = new AtomicBoolean();
  private final AtomicInteger maxpoll = new AtomicInteger(1000);

  private HazelcastTransactionalConsumerHandler<String> handler;

  @Before
  public void setup() throws InterruptedException {
    lenient().when(hazelcastInstance.newTransactionContext(any())).thenReturn(transactionContext);
    lenient().when(transactionContext.<String>getQueue(any())).thenReturn(txHazelcastQueue);
    lenient().when(txHazelcastQueue.poll(anyLong(), any())).thenReturn(null);
    when(workerPool.submit(isA(Runnable.class))).thenAnswer(i -> {
      workerTask.set(i.getArgument(0));
      return null;
    });

    //by default, only one round
    doAnswer(i -> shutdown.getAndSet(true)).when(transactionContext).commitTransaction();

    //allow a single iteration before shutting down
    when(workerPool.isShutdown()).thenAnswer(i -> {
      if (maxpoll.decrementAndGet() < 0) {
        return true;
      }
      return shutdown.get();
    });

    handler = new HazelcastTransactionalConsumerHandler<>(
            hazelcastInstance,
            HAZELCAST_QUEUE_NAME,
            () -> transactionalConsumer
    )
            .setWorkerPool(workerPool)
            .addWorkerLifecycleListener(workerLifecycleListener);
  }

  @Test
  public void workerStart() throws Exception {
    handler.startComponent();
    verify(workerPool).submit(isA(Runnable.class));
    assertNotNull(workerTask.get());
    handler.stopComponent();
    verify(workerPool).shutdown();
    verify(workerPool).awaitTermination(anyLong(), any());
  }

  @Test
  public void workerRun() throws InterruptedException, IOException, ConsumerGaveUpException {
    handler.startComponent();
    assertNotNull(workerTask.get());
    workerTask.get().run();
    verify(workerLifecycleListener, timeout(10000)).workerStopped(any());
    verify(transactionalConsumer, never()).consume(any());
  }

  @Test
  public void workerRunWithContent() throws Exception {
    when(txHazelcastQueue.poll(anyLong(), any())).thenReturn("testvalue");
    //only one round before shutting down
    doAnswer(i -> shutdown.getAndSet(true)).when(transactionalConsumer).consume(any());

    handler.startComponent();
    assertNotNull(workerTask.get());
    workerTask.get().run();
    verify(workerLifecycleListener, timeout(10000)).workerStopped(any());
    verify(transactionalConsumer).consume(argThat(c -> c.size() == 100));
  }

  @Test
  public void workerRunWithLimitedContent() throws Exception {
    when(txHazelcastQueue.poll(anyLong(), any()))
            .thenReturn("testvalue")
            .thenReturn("testvalue")
            .thenReturn(null);

    handler.startComponent();
    assertNotNull(workerTask.get());
    workerTask.get().run();
    verify(workerLifecycleListener, timeout(10000)).workerStopped(any());
    verify(transactionalConsumer).consume(argThat(c -> c.size() == 2));
  }

}