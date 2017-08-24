package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.testtools.MockitoTools;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetStreamInterruptedException;
import no.mnemonic.services.common.api.ServiceTimeOutException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static no.mnemonic.commons.testtools.MockitoTools.match;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceMessageClientTest {

  private static ExecutorService executor = Executors.newFixedThreadPool(10);

  @Mock
  private RequestSink requestSink;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @AfterClass
  public static void afterAll() {
    executor.shutdown();
  }

  @Test
  public void testRequest() {
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
  public void testPrimitiveTypes() {
    doTestPrimitiveType(()-> proxy().primitiveBooleanArgument(true), Boolean.TYPE.getName(), true);
    doTestPrimitiveType(()-> proxy().primitiveLongArgument(1), Long.TYPE.getName(), 1L);
    doTestPrimitiveType(()-> proxy().primitiveIntArgument(1), Integer.TYPE.getName(), 1);
    doTestPrimitiveType(()-> proxy().primitiveCharArgument('a'), Character.TYPE.getName(), 'a');
    doTestPrimitiveType(()-> proxy().primitiveFloatArgument((float)1.0), Float.TYPE.getName(), (float)1.0);
    doTestPrimitiveType(()-> proxy().primitiveDoubleArgument(1.0), Double.TYPE.getName(), 1.0);
    doTestPrimitiveType(()-> proxy().primitiveByteArgument((byte)1), Byte.TYPE.getName(), (byte)1);
  }

  @Test
  public void testPrimitiveArray() {
    mockSingleResponse();
    assertEquals("value", proxy().primitiveArrayArgument(new long[]{1L,2L,3L}));
    verify(requestSink).signal(MockitoTools.match(
            r->Objects.equals(r.getArgumentTypes()[0], "[J") && Arrays.equals((long[])r.getArguments()[0], new long[]{1L,2L,3L}),
            ServiceRequestMessage.class),
            any(), anyLong()
    );
  }

  @Test
  public void testObjectArray() {
    mockSingleResponse();
    assertEquals("value", proxy().objectArrayArgument(new String[]{"a","b","c"}));
    verify(requestSink).signal(MockitoTools.match(
            r->Objects.equals(r.getArgumentTypes()[0], String[].class.getName()) && Arrays.equals((String[])r.getArguments()[0], new String[]{"a","b","c"}),
            ServiceRequestMessage.class),
            any(), anyLong()
    );
  }

  @Test
  public void testSingleValueResponse() {
    mockSingleResponse();
    assertEquals("value", proxy().getString("arg"));
  }

  @Test(expected = ServiceTimeOutException.class)
  public void testSingleValueTimeout() {
    proxy().getString("arg");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSingleValueException() {
    mockNotifyError(new IllegalArgumentException());
    proxy().getString("arg");
  }

  @Test(expected = ServiceTimeOutException.class)
  public void testResultSetTimeout() {
    proxy().getResultSet("arg");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testResultSetException() {
    mockNotifyError(new IllegalArgumentException());
    proxy().getResultSet("arg");
  }

  @Test(expected = IllegalStateException.class)
  public void testResultSetSubclassWithoutExtenderFunction() {
    proxy().getMyResultSet("arg");
  }

  @Test
  public void testResultSetWithExtenderFunction() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    TestService proxy = ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink).setMaxWait(100)
            .withExtenderFunction(TestService.MyResultSet.class, rs->{
              return new TestService.MyResultSet() {
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
              };
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
  public void testResultSetSingleBatch() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test(expected = ResultSetStreamInterruptedException.class)
  public void testResultSetNonZeroInitialIndex() throws Throwable {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("a", "b", "c")));
    try {
      result.get(100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = ResultSetStreamInterruptedException.class)
  public void testResultSetOutOfOrderBatches() throws Throwable {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(2, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("a", "b", "c")));
    try {
      list(result.get(100, TimeUnit.MILLISECONDS).iterator());
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test(expected = ResultSetStreamInterruptedException.class)
  public void testResultSetMissingFinalMessage() throws Throwable {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get();
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("a", "b", "c")));
    try {
      list(result.get(100, TimeUnit.MILLISECONDS).iterator());
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testResultSetMultipleBatches() throws InterruptedException, ExecutionException, TimeoutException {
    Future<RequestContext> ctxref = mockResultSetResponse();
    Future<ResultSet<String>> result = invokeResultSet();
    RequestContext ctx = ctxref.get(1000, TimeUnit.MILLISECONDS);
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(0, list("a", "b", "c")));
    ctx.addResponse(ServiceStreamingResultSetResponseMessage.builder().build(1, list("d", "e", "f"), true));
    ctx.endOfStream();
    assertEquals(list("a", "b", "c", "d", "e", "f"), list(result.get(1000, TimeUnit.MILLISECONDS).iterator()));
  }

  @Test
  public void testResultSetMultipleBatchesWithInitialTimeout() throws InterruptedException, ExecutionException, TimeoutException {
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
  public void testResultSetMultipleBatchesWithStreamTimeout() throws InterruptedException, ExecutionException, TimeoutException {
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
  public void testResultSetReceivesStreamingResults() throws InterruptedException, ExecutionException, TimeoutException {
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
  public void testResultSetReceivesExceptionMidStream() throws InterruptedException, ExecutionException, TimeoutException {
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
    return ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink)
            .setMaxWait(100)
            .build()
            .getInstance();
  }

  private void mockSingleResponse() {
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      ServiceRequestMessage msg = i.getArgument(0);
      RequestContext ctx = i.getArgument(1);
      ctx.addResponse(ServiceResponseValueMessage.create(msg.getRequestID(), "value"));
      ctx.endOfStream();
      return ctx;
    });
  }

  private Future<RequestContext> mockResultSetResponse() {
    CompletableFuture<RequestContext> future = new CompletableFuture<>();
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      RequestContext ctx = i.getArgument(1);
      future.complete(ctx);
      return ctx;
    });
    return future;
  }

  private void mockNotifyError(Throwable ex) {
    when(requestSink.signal(any(), any(), anyLong())).thenAnswer(i -> {
      RequestContext ctx = i.getArgument(1);
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
            r->Objects.equals(r.getArgumentTypes()[0], expectedTypeName) && r.getArguments()[0].equals(expectedValue),
            ServiceRequestMessage.class),
            any(), anyLong()
    );
  }
}
