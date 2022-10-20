package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalQueue;
import no.mnemonic.services.common.hazelcast.consumer.exception.ConsumerGaveUpException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HazelcastTransactionalConsumerTest {

  private static final String HAZELCAST_QUEUE_NAME = "My.Queue";

  @Mock
  private HazelcastInstance hazelcastInstance;
  @Mock
  private TransactionContext transactionContext;
  @Mock
  private TransactionalQueue<String> txHazelcastQueue;
  @Mock
  private TransactionalConsumer<String> transactionalConsumer;

  private HazelcastTransactionalConsumer<String> handler;

  @Before
  public void setup() throws InterruptedException {
    lenient().when(hazelcastInstance.newTransactionContext(any())).thenReturn(transactionContext);
    lenient().when(transactionContext.<String>getQueue(any())).thenReturn(txHazelcastQueue);
    lenient().when(txHazelcastQueue.poll(anyLong(), any())).thenReturn(null);
    handler = new HazelcastTransactionalConsumer<>(hazelcastInstance, HAZELCAST_QUEUE_NAME);
  }

  @Test
  public void testConsumeWithoutContent() throws InterruptedException, IOException, ConsumerGaveUpException {
    handler.consumeNextBatch(transactionalConsumer);

    verify(hazelcastInstance).newTransactionContext(any());
    verify(transactionContext).beginTransaction();
    verify(transactionContext).getQueue(HAZELCAST_QUEUE_NAME);
    verify(txHazelcastQueue).poll(anyLong(), any());
    verify(transactionContext).commitTransaction();
  }

  @Test
  public void testConsumeWithContent() throws InterruptedException, IOException, ConsumerGaveUpException {
    when(txHazelcastQueue.poll(anyLong(), any())).thenReturn("testvalue");

    handler.consumeNextBatch(transactionalConsumer);

    verify(txHazelcastQueue, times(100)).poll(anyLong(), any());
    verify(transactionalConsumer).consume(argThat(c -> c.size() == 100));
  }

  @Test
  public void testConsumeWithLimitedContent() throws InterruptedException, IOException, ConsumerGaveUpException {
    when(txHazelcastQueue.poll(anyLong(), any()))
            .thenReturn("testvalue")
            .thenReturn("testvalue")
            .thenReturn(null);

    handler.consumeNextBatch(transactionalConsumer);

    verify(txHazelcastQueue, times(3)).poll(anyLong(), any());
    verify(transactionalConsumer).consume(argThat(c -> c.size() == 2));
  }

  @Test
  public void txRollbackOnError() throws Exception {
    when(txHazelcastQueue.poll(anyLong(), any())).thenReturn("testvalue");
    doThrow(new IOException()).when(transactionalConsumer).consume(any());

    handler.setKeepThreadAliveOnException(false);
    assertThrows(IOException.class, () -> handler.consumeNextBatch(transactionalConsumer));

    verify(transactionContext, times(1)).rollbackTransaction();
  }

  @Test
  public void txRollbackButDontThrowExceptionIfOptionIsSet() throws Exception {
    AtomicBoolean consumerCalled = new AtomicBoolean(false);
    when(txHazelcastQueue.poll(anyLong(), any())).thenReturn("testvalue");
    doThrow(new IOException())
            .doAnswer(inv -> {
              consumerCalled.set(true);
              return null;
            })
            .when(transactionalConsumer).consume(any());

    handler.setKeepThreadAliveOnException(true);

    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));

    verify(transactionContext, times(1)).rollbackTransaction();
    verify(transactionContext, times(1)).commitTransaction();
    assertTrue(consumerCalled.get());
  }

  @Test
  public void testRollbackTransactionAndGiveUpIfOptionIsSetAndConsumerGaveUp() throws IOException, InterruptedException, ConsumerGaveUpException {
    AtomicBoolean consumerCalled = new AtomicBoolean(false);
    when(txHazelcastQueue.poll(anyLong(), any())).thenReturn("testvalue");
    doThrow(new IOException())
            .doThrow(new ConsumerGaveUpException("error"))
            .doAnswer(inv -> {
              consumerCalled.set(true);
              return null;
            })
            .when(transactionalConsumer).consume(any());

    handler.setKeepThreadAliveOnException(true);
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertThrows(ConsumerGaveUpException.class, () -> handler.consumeNextBatch(transactionalConsumer));

    verify(transactionContext, times(2)).rollbackTransaction();
    verify(transactionContext, never()).commitTransaction();
    assertFalse(consumerCalled.get());
  }

  @Test
  public void txRollbackWithErrorThreshold() throws Exception {
    when(txHazelcastQueue.poll(anyLong(), any())).thenReturn("testvalue");
    doThrow(new IOException())
            .doThrow(new IOException())
            .doNothing()
            .doThrow(new IOException())
            .doThrow(new IOException())
            .when(transactionalConsumer).consume(any());

    handler.setPermittedConsecutiveErrors(1);

    // Below threshold, ignore error
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertEquals(0, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Above threshold, report error
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertEquals(1, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Resets error state
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertEquals(1, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Below threshold, ignore error
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertEquals(1, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Above threshold, report error
    assertDoesNotThrow(() -> handler.consumeNextBatch(transactionalConsumer));
    assertEquals(2, handler.getMetrics().getData("bulk.failed.count").intValue());
  }
}