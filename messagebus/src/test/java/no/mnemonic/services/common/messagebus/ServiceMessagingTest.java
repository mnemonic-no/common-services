package no.mnemonic.services.common.messagebus;


import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.utilities.collections.ListUtils;
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
import static org.mockito.ArgumentMatchers.*;
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
    when(testService.primitiveLongArgument(anyLong())).thenReturn("resultstring");
    when(testService.primitiveBooleanArgument(anyBoolean())).thenReturn("resultstring");
    when(testService.primitiveIntArgument(anyInt())).thenReturn("resultstring");
    when(testService.primitiveCharArgument(anyChar())).thenReturn("resultstring");
    when(testService.primitiveByteArgument(anyByte())).thenReturn("resultstring");
    when(testService.primitiveFloatArgument(anyFloat())).thenReturn("resultstring");
    when(testService.primitiveDoubleArgument(anyDouble())).thenReturn("resultstring");
    when(testService.primitiveArrayArgument(any())).thenReturn("resultstring");
    when(testService.objectArrayArgument(any())).thenReturn("resultstring");
    when(testService.getResultSet(any())).thenAnswer(i -> createResultSet(createResults(1000)));

    ServiceMessageHandler listener = ServiceMessageHandler.builder()
            .setService(testService)
            .setSessionFactory(sessionFactory)
            .setMaxConcurrentRequests(SERVER_THREADS)
            .setBatchSize(100)
            .build();

    JMSRequestSink requestSink = createJmsRequestSink();
    JMSRequestProxy requestProxy = createJMSProxy(listener, SERVER_THREADS);

    CompletableFuture<Void> waitForConnection = new CompletableFuture<>();
    requestProxy.addJMSRequestProxyConnectionListener(p -> waitForConnection.complete(null));

    client = ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink)
            .setMaxWait(5000)
            .build();

    container = ComponentContainer.create(listener, requestProxy, requestSink);
    container.initialize();
    waitForConnection.get(1000, TimeUnit.MILLISECONDS);
    //add some sleep to let AMQ advisory messages propagate
    Thread.sleep(1000);
  }

  @After
  public void teardown() throws InterruptedException {
    container.destroy();
  }

  @Test
  public void testRemoteValueRequest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.getString("resultstring"));
  }

  @Test
  public void testPrimitiveLongArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveLongArgument(1L));
  }

  @Test
  public void testPrimitiveIntArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveIntArgument(1));
  }

  @Test
  public void testPrimitiveBooleanArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveBooleanArgument(false));
  }

  @Test
  public void testPrimitiveCharArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveCharArgument('a'));
  }

  @Test
  public void testPrimitiveFloatArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveFloatArgument((float) 1.0));
  }

  @Test
  public void testPrimitiveByteArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveByteArgument((byte)1));
  }

  @Test
  public void testPrimitiveDoubleArgumentTest() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveDoubleArgument(1.0));
  }

  @Test
  public void testPrimitiveArrayArgument() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.primitiveArrayArgument(new long[]{1L,2L,3L}));
  }

  @Test
  public void testObjectArrayArgument() throws NotBoundException, RemoteException {
    TestService srv = client.getInstance();
    assertEquals("resultstring", srv.objectArrayArgument(new String[]{"a","b","c"}));
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
