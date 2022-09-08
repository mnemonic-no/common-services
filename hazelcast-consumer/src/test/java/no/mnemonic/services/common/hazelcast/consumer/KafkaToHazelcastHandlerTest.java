package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.transaction.TransactionContext;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.documentchannel.kafka.KafkaDocumentBatch;
import no.mnemonic.messaging.documentchannel.kafka.KafkaDocumentSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
  private HazelcastInstance hazelcastInstance;
  @Mock
  private KafkaDocumentBatch<String> batch;

  private final String document = "document";
  private KafkaToHazelcastHandler<String> handler;

  @Before
  public void setup() throws InterruptedException {
    when(hazelcastInstance.newTransactionContext(any())).thenReturn(transactionContext);
    when(transactionContext.getQueue(any())).thenReturn(transactionalQueue);
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
    assertFalse(handler.isAlive());
  }

  @Test
  public void documentReceivedOfferTimeoutWithKeepalive() throws Exception {
    handler.setKeepThreadAliveOnException(true);
    // offer timeout
    when(transactionalQueue.offer(any(), anyLong(), any())).thenReturn(false);
    handler.runSingle();
    verify(transactionContext).rollbackTransaction();
    verify(batch).reject();
    assertTrue(handler.isAlive());
  }
}