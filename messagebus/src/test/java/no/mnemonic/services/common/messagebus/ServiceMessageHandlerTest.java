package no.mnemonic.services.common.messagebus;

import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class ServiceMessageHandlerTest extends AbstractServiceMessageTest {

  private static final String METHOD_GET_STRING = "getString";
  private static final String METHOD_GET_RESULTSET = "getResultSet";

  @Mock
  private TestService testService;
  @Mock
  private ServiceSessionFactory sessionFactory;
  @Mock
  private ServiceSession session;
  @Mock
  private RequestContext signalContext;

  private BlockingDeque<ServiceResponseMessage> responses = new LinkedBlockingDeque<>();
  private CompletableFuture<Throwable> error = new CompletableFuture<>();
  private CompletableFuture<Void> endOfStream = new CompletableFuture<>();
  private AtomicLong keepAlive = new AtomicLong();
  private ExecutorService executor = Executors.newCachedThreadPool();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(testService.getString(any())).thenReturn("result");
    when(testService.getResultSet(any())).thenReturn(createResultSet(createResults(3)));
    when(sessionFactory.openSession()).thenReturn(session);

    when(signalContext.addResponse(any())).thenAnswer(i -> responses.add(i.getArgument(0)));
    when(signalContext.keepAlive(anyLong())).thenAnswer(i -> {
      keepAlive.set(i.getArgument(0));
      return !endOfStream.isDone();
    });
    doAnswer(i -> endOfStream.complete(null)).when(signalContext).endOfStream();
    doAnswer(i -> error.complete(i.getArgument(0))).when(signalContext).notifyError(any());
  }

  @After
  public void cleanup() {
    executor = Executors.newCachedThreadPool();
  }

  @Test
  public void testRequestInvokesMethod() throws InterruptedException, ExecutionException, TimeoutException {
    sendSignal(METHOD_GET_STRING);
    endOfStream.get(100, TimeUnit.MILLISECONDS);
    verify(testService).getString("arg");
  }

  @Test
  public void testRequestOpensSession() throws Exception {
    sendSignal(METHOD_GET_STRING);
    endOfStream.get(100, TimeUnit.MILLISECONDS);
    verify(sessionFactory).openSession();
    verify(session).close();
  }

  @Test
  public void testRequestSendsKeepAlive() throws Exception {
    CompletableFuture<String> result = new CompletableFuture<>();
    when(testService.getString(any())).thenAnswer(i->result.get());
    sendSignal(METHOD_GET_STRING);
    verify(signalContext, never()).keepAlive(anyLong());
    try {
      endOfStream.get(150, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException ignored) {}
    verify(signalContext).keepAlive(anyLong());
    assertTrue(keepAlive.get() > System.currentTimeMillis());
    result.complete("value");
    endOfStream.get(100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSingleValueResponse() throws InterruptedException, ExecutionException, TimeoutException {
    sendSignal(METHOD_GET_STRING);
    endOfStream.get(100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testSingleValueException() throws InterruptedException, ExecutionException, TimeoutException {
    when(testService.getString(any())).thenThrow(new IllegalArgumentException("error"));
    sendSignal(METHOD_GET_STRING);
    endOfStream.get(100, TimeUnit.MILLISECONDS);
    assertEquals("error", error.get(100, TimeUnit.MILLISECONDS).getMessage());
  }

  @Test
  public void testResultSetResponse() throws InvocationTargetException, InterruptedException, ExecutionException, TimeoutException {
    sendSignal(METHOD_GET_RESULTSET);
    endOfStream.get(100, TimeUnit.MILLISECONDS);
    ServiceStreamingResultSetResponseMessage response = (ServiceStreamingResultSetResponseMessage) responses.poll();
    assertTrue(responses.isEmpty());
    assertEquals(10, response.getOffset());
    assertEquals(1000, response.getCount());
    assertEquals(100, response.getLimit());
    assertEquals(list("val0", "val1", "val2"), response.getBatch());
  }

  @Test
  public void testResultSetStreamingResponse() throws InvocationTargetException, InterruptedException, ExecutionException, TimeoutException {
    when(testService.getResultSet(any())).thenReturn(createResultSet(createResults(6)));
    sendSignal(METHOD_GET_RESULTSET);
    endOfStream.get(100, TimeUnit.MILLISECONDS);
    ServiceStreamingResultSetResponseMessage response = (ServiceStreamingResultSetResponseMessage) responses.poll();
    assertEquals(list("val0", "val1", "val2", "val3", "val4"), response.getBatch());

    response = (ServiceStreamingResultSetResponseMessage) responses.poll();
    assertNotNull(response);
    assertTrue(responses.isEmpty());
    assertEquals(list("val5"), response.getBatch());
  }

  @Test
  public void testResultSetStreamingResponseFromStreamingSource() throws InvocationTargetException, InterruptedException, TimeoutException, ExecutionException {
    BlockingDeque<String> queue = new LinkedBlockingDeque<>();
    AtomicBoolean finished = new AtomicBoolean();
    when(testService.getResultSet(any())).thenAnswer(i -> createBlockingResultSet(queue, finished::get));
    sendSignal(METHOD_GET_RESULTSET);

    //start releasing responses from blocking service
    for (int i = 0; i < 6; i++) {
      queue.add("val" + i);
    }
    ServiceStreamingResultSetResponseMessage response = (ServiceStreamingResultSetResponseMessage) responses.poll(1000, TimeUnit.MILLISECONDS);
    //assert that initial response has max batchSize values
    assertNotNull(response);
    assertEquals(list("val0", "val1", "val2", "val3", "val4"), response.getBatch());

    //release final result from service
    queue.add("val6");
    finished.set(true);

    //wait for response message to come through
    response = (ServiceStreamingResultSetResponseMessage) responses.poll(1000, TimeUnit.MILLISECONDS);
    assertNotNull(response);

    //wait for EOS from handler
    endOfStream.get(1000, TimeUnit.MILLISECONDS);
    //verify that response queue is empty
    assertTrue(responses.isEmpty());

    //verify final response
    assertEquals(list("val5", "val6"), response.getBatch());
  }

  //helpers

  private void sendSignal(String method) {
    ServiceMessageHandler handler = createHandler();
    ServiceRequestMessage req = createRequest(method).build();
    executor.submit(() ->
            handler.signal(req, signalContext, 1000)
    );
  }

  private ServiceRequestMessage.Builder createRequest(String method) {
    return ServiceRequestMessage.builder()
            .setRequestID("callid")
            .setServiceName("servicename")
            .setMethodName(method)
            .setArgumentTypes(new Class[]{String.class})
            .setArguments(new Object[]{"arg"});
  }

  private ServiceMessageHandler createHandler() {
    ServiceMessageHandler handler = ServiceMessageHandler.builder()
            .setService(testService)
            .setSessionFactory(sessionFactory)
            .setBatchSize(5)
            .setKeepAliveInterval(100)
            .build();
    handler.startComponent();
    return handler;
  }

  private ResultSet<String> createBlockingResultSet(BlockingQueue<String> queue, Supplier<Boolean> finished) {
    AtomicReference<String> ref = new AtomicReference<>();
    AtomicInteger sizeref = new AtomicInteger(0);
    Iterator<String> blockingIterator = new Iterator<String>() {
      @Override
      public boolean hasNext() {
        if (ref.get() != null) return true;
        while (!finished.get()) {
          if (ref.get() != null) return true;
          try {
            ref.set(queue.poll(1000, TimeUnit.MILLISECONDS));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return ref.get() != null;
      }

      @Override
      public String next() {
        sizeref.incrementAndGet();
        return ref.getAndSet(null);
      }
    };
    return createResultSet(blockingIterator);
  }


}
