package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalQueue;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.documentchannel.kafka.KafkaDocumentBatch;
import no.mnemonic.messaging.documentchannel.kafka.KafkaDocumentSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToHazelcastHandlerTest {

  @Mock
  private KafkaDocumentSource<String> source;
  @Mock
  private TransactionContext transactionContext;
  @Mock
  private TransactionalQueue<Object> transactionalQueue;
  @Mock
  private IQueue<Object> queue;
  @Mock
  private HazelcastInstance hazelcastInstance;
  @Mock
  private KafkaDocumentBatch<String> batch;

  private final String document = "document";
  private KafkaToHazelcastHandler<String> handler;

  @Before
  public void setup() throws InterruptedException {
    when(hazelcastInstance.newTransactionContext(any())).thenReturn(transactionContext);
    when(transactionContext.getQueue(any())).thenReturn(transactionalQueue);
    when(hazelcastInstance.getQueue(any())).thenReturn(queue);
    when(source.poll(any())).thenReturn(batch);
    when(batch.getDocuments()).thenReturn(ListUtils.list(document));
    when(transactionalQueue.offer(any(), anyLong(), any())).thenReturn(true);
    handler = new KafkaToHazelcastHandler<>(source, hazelcastInstance, "Queue")
            .setHazelcastQueueOfferTimeoutSec(1L);
  }

  @Test
  public void testBatch() throws Exception {
    handler.runSingle();
    verify(transactionalQueue).offer(eq(document), eq(1L), eq(TimeUnit.SECONDS));
    verify(transactionContext).commitTransaction();
    verify(batch).acknowledge();
  }

  @Test
  public void documentReceivedOfferTimeout() throws Exception {
    // offer timeout
    when(transactionalQueue.offer(any(), anyLong(), any())).thenReturn(false);
    handler.runSingle();
    verify(transactionContext).rollbackTransaction();
    verify(batch).reject();
    assertTrue(handler.isAlive());
  }

  @Test
  public void documentReceivedOfferUnexpectedException() throws Exception {
    handler.setKeepThreadAliveOnException(false);
    when(transactionalQueue.offer(any(), anyLong(), any())).thenThrow(RuntimeException.class);
    assertThrows(RuntimeException.class, () -> handler.runSingle());
    verify(transactionContext).rollbackTransaction();
    verify(batch).reject();
    assertFalse(handler.isAlive());
  }

  @Test
  public void documentReceivedOfferUnexpectedExceptionKeepAliveTrue() throws Exception {
    handler.setKeepThreadAliveOnException(true);
    when(transactionalQueue.offer(any(), anyLong(), any())).thenThrow(RuntimeException.class);
    assertDoesNotThrow(() -> handler.runSingle());
    verify(transactionContext).rollbackTransaction();
    verify(batch).reject();
    assertTrue(handler.isAlive());
  }

  @Test
  public void documentReceivedWithErrorThreshold() throws Exception {
    handler.setPermittedConsecutiveErrors(1);
    when(transactionalQueue.offer(any(), anyLong(), any()))
            .thenThrow(RuntimeException.class)
            .thenThrow(RuntimeException.class)
            .thenReturn(true)
            .thenThrow(RuntimeException.class)
            .thenThrow(RuntimeException.class);

    // Below threshold, ignore error
    assertDoesNotThrow(() -> handler.runSingle());
    assertEquals(0, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Above threshold, report error
    assertDoesNotThrow(() -> handler.runSingle());
    assertEquals(1, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Resets error state
    assertDoesNotThrow(() -> handler.runSingle());
    assertEquals(1, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Below threshold, ignore error
    assertDoesNotThrow(() -> handler.runSingle());
    assertEquals(1, handler.getMetrics().getData("bulk.failed.count").intValue());

    // Above threshold, report error
    assertDoesNotThrow(() -> handler.runSingle());
    assertEquals(2, handler.getMetrics().getData("bulk.failed.count").intValue());
  }
}