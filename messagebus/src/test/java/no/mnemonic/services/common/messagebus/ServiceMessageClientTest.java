package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.testtools.MockitoTools;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestListener;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetStreamInterruptedException;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.ServiceTimeOutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static no.mnemonic.commons.testtools.MockitoTools.match;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServiceMessageClientTest {

  private static final ExecutorService executor = Executors.newFixedThreadPool(10);

  @Mock
  private RequestSink requestSink;
  @Mock
  private RequestListener requestListener;

  @AfterAll
  public static void afterAll() {
    executor.shutdown();
  }

  @AfterEach
  void tearDown() {
    ServiceMessageClient.setGlobalThreadPriority(null);
  }

  @Test
  void testRequest() {
    mockSingleResponse();
    assertEquals("value", proxy().getString("arg"));
    verify(requestSink).signal(match(r ->
                    r.getServiceName().equals(TestService.class.getName())
                            && r.getMethodName().equals("getString")
                            && r.getArgumentTypes().length == 1 && Objects.equals(r.getArgumentTypes()[0], String.class.getName())
                            && r.getArguments().length == 1 && r.getArguments()[0].equals("arg"),
            ServiceRequestMessage.class), any(), eq(100L));
  }

  @Test
  void testDefaultPriority() {
    mockSingleResponse();
    proxy().getString("arg");
    verify(requestSink).signal(match(r -> r.getPriority() == Message.Priority.standard, ServiceRequestMessage.class), any(), anyLong());
  }

  @Test
  void testSetDefaultPriority() {
    mockSingleResponse();
    TestService srv = proxyBuilder().setDefaultPriority(ServiceContext.Priority.bulk).build().getInstance();
    srv.getString("arg");
    verify(requestSink).signal(match(r -> r.getPriority() == Message.Priority.bulk, ServiceRequestMessage.class), any(), anyLong());
  }

  @Test
  void testSetThreadPriority() {
    mockSingleResponse();
    TestService srv = proxy();
    srv.getServiceContext().setThreadPriority(ServiceContext.Priority.bulk);
    srv.getString("arg");
    srv.getString("arg");
    verify(requestSink, times(2)).signal(match(r -> r.getPriority() == Message.Priority.bulk, ServiceRequestMessage.class), any(), anyLong());
  }

  @Test
  void testGetGlobalThreadPriority() {
    assertEquals(ServiceContext.Priority.standard, ServiceMessageClient.getGlobalThreadPriority());
    ServiceMessageClient.setGlobalThreadPriority(ServiceContext.Priority.bulk);
    assertEquals(ServiceContext.Priority.bulk, ServiceMessageClient.getGlobalThreadPriority());
  }

  @Test
  void testSetGlobalThreadPriority() {
    mockSingleResponse();
    TestService srv = proxy();
    ServiceMessageClient.setGlobalThreadPriority(ServiceContext.Priority.bulk);
    srv.getString("arg");
    srv.getString("arg");
    verify(requestSink, times(2)).signal(match(r -> r.getPriority() == Message.Priority.bulk, ServiceRequestMessage.class), any(), anyLong());
  }

  @Test
  void testSetNextPriority() {
    mockSingleResponse();
    TestService srv = proxy();
    srv.getServiceContext().setNextRequestPriority(ServiceContext.Priority.bulk);
    srv.getString("arg");
    srv.getString("arg");
    verify(requestSink).signal(match(r -> r.getPriority() == Message.Priority.bulk, ServiceRequestMessage.class), any(), anyLong());
    verify(requestSink).signal(match(r -> r.getPriority() == Message.Priority.standard, ServiceRequestMessage.class), any(), anyLong());
  }

  @Test
  void testPrimitiveTypes() {
    doTestPrimitiveType(() -> proxy().primitiveBooleanArgument(true), Boolean.TYPE.getName(), true);
    doTestPrimitiveType(() -> proxy().primitiveLongArgument(1), Long.TYPE.getName(), 1L);
    doTestPrimitiveType(() -> proxy().primitiveIntArgument(1), Integer.TYPE.getName(), 1);
    doTestPrimitiveType(() -> proxy().primitiveCharArgument('a'), Character.TYPE.getName(), 'a');
    doTestPrimitiveType(() -> proxy().primitiveFloatArgument((float) 1.0), Float.TYPE.getName(), (float) 1.0);
    doTestPrimitiveType(() -> proxy().primitiveDoubleArgument(1.0), Double.TYPE.getName(), 1.0);
    doTestPrimitiveType(() -> proxy().primitiveByteArgument((byte) 1), Byte.TYPE.getName(), (byte) 1);
  }

  @Test
  void testPrimitiveArray() {
    mockSingleResponse();
    assertEquals("value", proxy().primitiveArrayArgument(new long[]{1L, 2L, 3L}));
    verify(requestSink).signal(MockitoTools.match(
            r -> Objects.equals(r.getArgumentTypes()[0], "[J") && Arrays.equals((long[]) r.getArguments()[0], new long[]{1L, 2L, 3L}),
            ServiceRequestMessage.class),
            any(), anyLong()
    );
  }

  @Test
  void testObjectArray() {
    mockSingleResponse();
    assertEquals("value", proxy().objectArrayArgument(new String[]{"a", "b", "c"}));
    verify(requestSink).signal(MockitoTools.match(
            r -> Objects.equals(r.getArgumentTypes()[0], String[].class.getName()) && Arrays.equals((String[]) r.getArguments()[0], new String[]{"a", "b", "c"}),
            ServiceRequestMessage.class),
            any(), anyLong()
    );
  }

  @Test
  void testSingleValueResponse() {
    mockSingleResponse();
    assertEquals("value", proxy().getString("arg"));
  }

  @Test
  void testInvocationMetrics() throws MetricException {
    mockSingleResponse();
    ServiceMessageClient<TestService> client = ServiceMessageClient.builder(TestService.class).setRequestSink(requestSink).setMaxWait(100).build();
    assertEquals(0L, client.getMetrics().getData("requests").longValue());
    TestService service = client.getInstance();
    assertEquals("value", service.getString("arg"));
    assertEquals(1L, client.getMetrics().getData("requests").longValue());
  }

  @Test
  void testErrorMetricsOnInitialTimeout() throws MetricException {
    ServiceMessageClient<TestService> client = ServiceMessageClient.builder(TestService.class).setMaxWait(100).setRequestSink(requestSink).build();
    TestService service = client.getInstance();
    assertFalse(LambdaUtils.tryTo(() -> service.getString("arg")));
    assertEquals(1L, client.getMetrics().getData("serviceTimeOuts").longValue());
  }

  @Test
  void testErrorMetricsOnStreamInterrupted() throws InterruptedException, ExecutionException, TimeoutException, MetricException {
    ServiceMessageClient<TestService> client = ServiceMessageClient.builder(TestService.class).setMaxWait(500).setRequestSink(requestSink).build();
    Future<RequestContext> ctxref = mockResultSetResponse();
    //invoke a resultset
    Future<ResultSet<String>> result = executor.submit(() -> client.getInstance().getResultSet("arg"));
    RequestContext ctx = ctxref.get(200, TimeUnit.MILLISECONDS);
    //add initial response
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ResultSet<String> rs = result.get(100, TimeUnit.MILLISECONDS);
    //verify that we got an error when iterating the stream (due to timeout)
    assertFalse(LambdaUtils.tryTo(() -> ListUtils.list(rs.iterator())));
    //verify that the error was counted correctly
    assertEquals(1L, client.getMetrics().getData("streamingInterrupted").longValue());
  }

  @Test
  void testRequestContextClosedOnResultSetClose() throws InterruptedException, ExecutionException, TimeoutException, MetricException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c"), true));
    ResultSet<String> resultset = result.get(100, TimeUnit.MILLISECONDS);
    resultset.close();
    assertTrue(ctx.isClosed());
    verify(requestListener).close(any());
  }

  @Test
  void testSingleValueTimeout() {
    assertThrows(ServiceTimeOutException.class, () -> proxy().getString("arg"));
  }

  @Test
  void testSingleValueTimeoutSignalsRequestSinkTimeout() {
    mockNoResponse();
    assertThrows(ServiceTimeOutException.class, () -> proxy().getString("arg"));
    verify(requestListener).timeout();
  }

  @Test
  void testSingleValueException() {
    mockNotifyError(new IllegalArgumentException());
    assertThrows(IllegalArgumentException.class, () -> proxy().getString("arg"));
  }

  @Test
  void testResultSetTimeout() {
    assertThrows(ServiceTimeOutException.class, () -> proxy().getResultSet("arg"));
  }

  @Test
  void testResultSetTimeoutSignalsRequestSinkTimeout() {
    mockNoResponse();
    try {
      proxy().getResultSet("arg");
    } catch (ServiceTimeOutException ignored) {
      verify(requestListener).timeout();
    }
  }

  @Test
  void testResultSetException() {
    mockNotifyError(new IllegalArgumentException());
    assertThrows(IllegalArgumentException.class, () -> proxy().getResultSet("arg"));
  }

  @Test
  void testResultSetSubclassWithoutExtenderFunction() {
    assertThrows(IllegalStateException.class, () -> proxy().getMyResultSet("arg"));
  }

  @Test
  void testResultSetSubclassWithAnnotatedExtender() throws ExecutionException, InterruptedException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = executor.submit(() -> proxy().getMyAnnotatedResultSet("arg"));
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetWithExtenderFunction() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    TestService proxy = ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink).setMaxWait(100)
            .withExtenderFunction(TestService.MyResultSet.class, (rs, metaData) -> new TestService.MyResultSet() {
              @Override
              public int getCount() {
                return rs.getCount();
              }

              @Override
              public int getLimit() {
                return rs.getLimit();
              }

              @Override
              public int getOffset() {
                return rs.getOffset();
              }

              @Override
              public Iterator iterator() throws ResultSetStreamInterruptedException {
                return rs.iterator();
              }
            })
            .build()
            .getInstance();

    Future<ResultSet<String>> result = executor.submit(() -> proxy.getMyResultSet("arg"));
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetSingleBatch() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetNonZeroInitialIndex() throws Throwable {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("a", "b", "c")));
    ExecutionException ex = assertThrows(ExecutionException.class, () -> result.get(100, TimeUnit.MILLISECONDS));
    assertTrue(ex.getCause() instanceof ResultSetStreamInterruptedException);
  }

  @Test
  void testResultSetOutOfOrderBatches() throws Throwable {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(2, list("g", "h", "i"), true));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("d", "e", "f")));
    assertEquals(list("a", "b", "c", "d", "e", "f", "g", "h", "i"), list(result.get(100, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetMissingFinalMessage() throws Throwable {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("d", "e", "f")));
    assertThrows(ResultSetStreamInterruptedException.class, ()->list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetMultipleBatches() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get(1000, TimeUnit.MILLISECONDS);
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("d", "e", "f"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c", "d", "e", "f"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetMultipleBatchesWithInitialTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get(1000, TimeUnit.MILLISECONDS);
    assertTrue(ctx.keepAlive(System.currentTimeMillis() + 10000));
    Thread.sleep(1000);
    assertFalse(ctx.isClosed());
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetMultipleBatchesWithStreamTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get(1000, TimeUnit.MILLISECONDS);
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.keepAlive(10000);
    Thread.sleep(1000);
    assertFalse(ctx.isClosed());
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("d", "e", "f"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c", "d", "e", "f"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  void testResultSetReceivesStreamingResults() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get(1000, TimeUnit.MILLISECONDS);
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));

    Iterator<String> iter = result.get(1000, TimeUnit.MILLISECONDS).iterator();
    assertEquals("a", iter.next());
    assertEquals("b", iter.next());
    assertEquals("c", iter.next());

    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("d", "e", "f"), true));
    ctx.endOfStream();

    assertEquals("d", iter.next());
    assertEquals("e", iter.next());
    assertEquals("f", iter.next());
    assertFalse(iter.hasNext());
  }

  @Test
  void testResultSetReceivesExceptionMidStream() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get(1000, TimeUnit.MILLISECONDS);
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a")));
    Iterator<String> iter = result.get(1000, TimeUnit.MILLISECONDS).iterator();
    assertEquals("a", iter.next());
    ctx.notifyError(new IllegalArgumentException("exception"));
    try {
      iter.next();
      fail();
    } catch (ResultSetStreamInterruptedException ignored) {
    }
  }

  //helpers

  private Future<ResultSet<String>> invokeResultSet() {
    return executor.submit(() -> proxy().getResultSet("arg"));
  }

  private TestService proxy() {
    return proxyBuilder()
            .build()
            .getInstance();
  }

  private ServiceMessageClient.Builder<TestService> proxyBuilder() {
    return ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink)
            .setMaxWait(100);
  }

  private void mockNoResponse() {
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      RequestContext ctx = i.getArgument(1);
      ctx.addListener(requestListener);
      return ctx;
    });
  }

  private void mockSingleResponse() {
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      ServiceRequestMessage msg = i.getArgument(0);
      RequestContext ctx = i.getArgument(1);
      ctx.addListener(requestListener);
      ctx.addResponse(ServiceResponseValueMessage.create(msg.getRequestID(), "value"));
      ctx.endOfStream();
      return ctx;
    });
  }

  private Future<RequestContext> mockResultSetResponse() {
    CompletableFuture<RequestContext> future = new CompletableFuture<>();
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      RequestContext ctx = i.getArgument(1);
      ctx.addListener(requestListener);
      future.complete(ctx);
      return ctx;
    });
    return future;
  }

  private void mockNotifyError(Throwable ex) {
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      RequestContext ctx = i.getArgument(1);
      ctx.addListener(requestListener);
      ctx.notifyError(ex);
      ctx.endOfStream();
      return ctx;
    });
  }


  private void doTestPrimitiveType(Supplier<String> testFunction, String expectedTypeName, Object expectedValue) {
    reset(requestSink);
    mockSingleResponse();
    assertEquals("value", testFunction.get());
    verify(requestSink).signal(MockitoTools.match(
            r -> Objects.equals(r.getArgumentTypes()[0], expectedTypeName) && r.getArguments()[0].equals(expectedValue),
            ServiceRequestMessage.class),
            any(), anyLong()
    );
  }
}
