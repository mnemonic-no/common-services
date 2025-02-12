package no.mnemonic.services.common.api.proxy.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AllArgsConstructor;
import no.mnemonic.commons.utilities.StreamUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import no.mnemonic.services.common.api.proxy.*;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import no.mnemonic.services.common.api.proxy.messages.ServiceResponseMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;
import no.mnemonic.services.common.api.proxy.serializer.XStreamSerializer;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ServiceProxyTest {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  public static final String BASE_URL = "http://localhost:9001";
  public static final int MAX_READ_STRING_LENGTH = 100_000;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private ServiceProxy proxy;
  @Mock
  private TestService service;
  @Mock
  private ServiceSessionFactory sessionFactory;
  @Mock
  private ServiceSession session;

  private final Serializer serializer = XStreamSerializer.builder()
          .setAllowedClass(TestException.class)
          .setAllowedClass(TestArgument.class)
          .build();
  private ServiceInvocationHandler<TestService> invocationHandler;

  @BeforeEach
  void setUp() {
    lenient().when(sessionFactory.openSession()).thenReturn(session);
    invocationHandler = ServiceInvocationHandler.<TestService>builder()
            .setProxiedService(service)
            .addSerializer(serializer)
            .setSessionFactory(sessionFactory)
            .setExecutorService(executorService)
            .build();
    proxy = ServiceProxy.builder()
            .addInvocationHandler(TestService.class, invocationHandler)
            .setReadMaxStringLength(MAX_READ_STRING_LENGTH)
            .build();
  }

  @AfterEach
  void tearDown() {
    proxy.stopComponent();
    executorService.shutdown();
  }

  @Test
  void testCircuitBreaker() throws TestException, ExecutionException, InterruptedException, TimeoutException {
    proxy = ServiceProxy.builder()
            .addInvocationHandler(TestService.class, invocationHandler)
            .setStandardThreads(3)
            .setCircuitBreakerLimit(1)
            .setReadMaxStringLength(MAX_READ_STRING_LENGTH)
            .build();
    proxy.startComponent();
    CountDownLatch latch = new CountDownLatch(1);
    when(service.getString(any())).thenAnswer(i -> {
      latch.await(20, TimeUnit.SECONDS);
      return "answer";
    });
    ExecutorService exec = Executors.newCachedThreadPool();

    //start 10 requests (with a tiny offset between each request, to avoid filling the threadpool immediately, which gives an unstable test
    List<Future<Response>> responses = ListUtils.list();
    for (int i = 0; i < 10; i++) {
      responses.add(exec.submit(() -> invoke(false, "getString", "stringArg")));
      Thread.sleep(100);
    }

    //let it simmer a bit, just to make sure all requests are scheduled
    Thread.sleep(1000);

    //release requests, so requests which are not already rejected are allowed to complete
    latch.countDown();

    //fetch results from all requests and summarize
    int oks = 0;
    int errors = 0;
    for (Future<Response> f : responses) {
      int code = f.get(20, TimeUnit.SECONDS).code;
      if (code == 200) oks++;
      else if (code == 503) errors++;
      else fail();
    }

    //expect errors and more errors than oks
    assertTrue(errors > 0);
    assertTrue(errors > oks);
  }

  @Test
  void testSimpleInvocation() throws IOException, TestException {
    proxy.startComponent();
    when(service.getString(any())).thenReturn("returnString");
    Response response = invoke(false, "getString", "stringArg");
    assertEquals(Utils.HTTP_OK_RESPONSE, response.code);
    verify(service).getString("stringArg");
    assertEquals("returnString", readResponse(response));
  }

  @Test
  void testResultSetInvocation() throws IOException, TestException {
    proxy.startComponent();
    when(service.getResultSet(any())).thenReturn(ResultSetImpl.<String>builder()
            .setIterator(list("a", "b", "c").iterator())
            .setLimit(5).setOffset(10).setCount(20)
            .build());
    Response response = invoke(true, "getResultSet", "stringArg");
    assertEquals(Utils.HTTP_OK_RESPONSE, response.code);
    verify(service).getResultSet("stringArg");
    ResultSet<Object> resultSet = readResultSet(response);
    assertEquals(5, resultSet.getLimit());
    assertEquals(10, resultSet.getOffset());
    assertEquals(20, resultSet.getCount());
    assertEquals(list("a", "b", "c"), list(resultSet.iterator()));
  }

  @Test
  void testServiceException() throws IOException, TestException {
    proxy.startComponent();
    when(service.getString(any())).thenThrow(TestException.class);
    Response response = invoke(false, "getString", "stringArg");
    assertEquals(Utils.HTTP_OK_RESPONSE, response.code);
    verify(service).getString("stringArg");
    assertInstanceOf(TestException.class, readException(response));
  }

  @Test
  void testLargeMessage() throws IOException, TestException {
    proxy.startComponent();
    String largeString = Stream.generate(() -> "a").limit(MAX_READ_STRING_LENGTH / 2).collect(Collectors.joining());
    Response response = invoke(false, "getString", largeString);
    assertEquals(Utils.HTTP_OK_RESPONSE, response.code);
    verify(service).getString(largeString);
  }

  @Test
  void testTooLargeMessage() throws IOException, TestException {
    proxy.startComponent();
    String largeString = Stream.generate(() -> "a").limit(MAX_READ_STRING_LENGTH + 10).collect(Collectors.joining());
    Response response = invoke(false, "getString", largeString);
    assertEquals(Utils.HTTP_ERROR_RESPONSE, response.code);
    verify(service, never()).getString(any());
  }

  private <T> T readResponse(Response response) throws IOException {
    ServiceResponseMessage responseMessage = MAPPER.readValue(response.data, ServiceResponseMessage.class);
    assertNotNull(responseMessage.getResponse());
    return serializer.deserializeB64(responseMessage.getResponse());
  }

  private <T> ResultSet<T> readResultSet(Response response) throws IOException {
    JsonNode resultset = MAPPER.readTree(response.data);
    ResultSetImpl.ResultSetImplBuilder<T> builder = ResultSetImpl.<T>builder()
            .setCount(resultset.get("count").asInt())
            .setLimit(resultset.get("limit").asInt())
            .setOffset(resultset.get("offset").asInt());
    List<T> data = new ArrayList<>();
    for (int i = 0; i < resultset.get("data").size(); i++) {
      data.add(serializer.deserializeB64(resultset.get("data").get(i).asText()));
    }
    return builder.setIterator(data.iterator()).build();
  }

  private <T extends Exception> T readException(Response response) throws IOException {
    ServiceResponseMessage responseMessage = MAPPER.readValue(response.data, ServiceResponseMessage.class);
    assertNotNull(responseMessage.getException());
    return serializer.deserializeB64(responseMessage.getException());
  }

  private Response invoke(boolean resultset, String method, Object... arguments) throws IOException {
    ServiceRequestMessage.ServiceRequestMessageBuilder requestBuilder = ServiceRequestMessage.builder()
            .setSerializerID(serializer.serializerID());
    if (arguments != null) {
      for (Object arg : arguments) {
        requestBuilder
                .addArgument(arg.getClass(), serialize(arg));
      }
    }
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost request = new HttpPost(URI.create(
              String.format("%s/service/v1/%s/%s/%s", BASE_URL, TestService.class.getName(), resultset ? "resultset" : "single", method)
      ));
      request.setEntity(new StringEntity(MAPPER.writeValueAsString(requestBuilder.build())));
      return httpClient.execute(request, resp -> new Response(resp.getCode(), StreamUtils.readFullStream(resp.getEntity().getContent(), true)));
    }

  }

  private String serialize(Object serializedValue) throws IOException {
    return serializer.serializeB64(serializedValue);
  }

  @AllArgsConstructor
  private static class Response {
    final int code;
    final byte[] data;
  }

}