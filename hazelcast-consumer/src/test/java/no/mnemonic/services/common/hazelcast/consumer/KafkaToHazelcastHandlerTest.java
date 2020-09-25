package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToHazelcastHandlerTest {

  @Mock
  private DocumentSource<String> source;
  @Mock
  private DocumentChannelSubscription subscription;
  @Mock
  private IQueue<String> hazelcastQueue;
  @Mock
  private HazelcastInstance hazelcastInstance;

  private final String document = "document";
  private KafkaToHazelcastHandler<String> handler;

  @Before
  public void setup() {
    when(hazelcastInstance.<String>getQueue(any())).thenReturn(hazelcastQueue);
    when(source.createDocumentSubscription(any())).thenReturn(subscription);
    handler = new KafkaToHazelcastHandler<>(source, hazelcastInstance, "Queue")
            .setHazelcastQueueOfferTimeoutSec(1L);
  }

  @Test
  public void startComponent() {
    handler.startComponent();
    verify(source).createDocumentSubscription(eq(handler));
  }

  @Test
  public void stopComponent() {
    handler.startComponent();
    handler.stopComponent();
    verify(subscription).cancel();
  }

  @Test
  public void documentReceived() throws Exception {
    when(hazelcastQueue.offer(any(), anyLong(), any())).thenReturn(true);

    handler.documentReceived(document);
    verify(hazelcastQueue).offer(eq(document), eq(1L), eq(TimeUnit.SECONDS));
  }

  @Test
  public void documentReceivedOfferTimeout() throws Exception {
    // offer timeout
    when(hazelcastQueue.offer(any(), anyLong(), any())).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> handler.documentReceived(document));
  }
}