package no.mnemonic.services.common.messagebus;


import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.requestsink.jms.JMSConnection;
import no.mnemonic.messaging.requestsink.jms.JMSRequestProxy;
import no.mnemonic.messaging.requestsink.jms.JMSRequestSink;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class ServiceMessagingTest  extends AbstractServiceMessagePerformanceTest {

  private static final int SERVER_THREADS = 10;

  private ServiceMessageClient<TestService> client;
  private ComponentContainer container;

  @Mock
  private TestService testService;
  @Mock
  private ServiceSessionFactory sessionFactory;

  @Before
  public void setup() throws InterruptedException, ExecutionException, TimeoutException {
    MockitoAnnotations.initMocks(this);
    when(sessionFactory.openSession()).thenReturn(() -> {});
    when(testService.getString(any())).thenAnswer(i -> i.getArgument(0));
    when(testService.getResultSet(any())).thenAnswer(i -> createResultSet(createResults(1000)));

    ServiceMessageHandler listener = ServiceMessageHandler.builder()
            .setService(testService)
            .setSessionFactory(sessionFactory)
            .setMaxConcurrentRequests(SERVER_THREADS)
            .setBatchSize(100)
            .build();

    JMSConnection connection = createJMSConnection();
    JMSRequestSink requestSink = createJmsRequestSink(connection);
    JMSRequestProxy requestProxy = createJMSProxy(connection, listener, SERVER_THREADS);

    CompletableFuture<Void> waitForConnection = new CompletableFuture<>();
    requestProxy.addJMSRequestProxyConnectionListener(p -> waitForConnection.complete(null));

    client = ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink)
            .setMaxWait(1000)
            .build();

    container = ComponentContainer.create(connection, listener, requestProxy, requestSink);
    container.initialize();
    waitForConnection.get(1000, TimeUnit.MILLISECONDS);
  }

  @After
  public void teardown() {
    container.destroy();
  }

  @Test
  public void testRemoteValueRequest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.getString("resultstring"));
  }

  @Test
  public void testRemoteResultSetRequest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    ResultSet<String> rs = srv.getResultSet("arg");
    List<String> result = ListUtils.list(rs.iterator());
    assertEquals(1000, result.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRemoteException() throws NotBoundException, RemoteException {
    when(testService.getString(any())).thenAnswer(i -> {
      System.out.println("Got request to server mock");
      throw new IllegalArgumentException("Illegal argument");
    });
    TestService srv = client.getInstance();
    srv.getString("string");
  }

  @Test
  public void testRemoteRequestSerialTiming() throws NotBoundException, RemoteException {
    int count = 100;
    TestService srv = client.getInstance();

    long start = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      srv.getString("str" + i);
    }
    long end = System.currentTimeMillis();
    long time = end - start;
    System.out.println(String.format("Executed %d invocations in %d ms (%.2f ms/req)", count, time, ((double) time) / count));
  }

}
