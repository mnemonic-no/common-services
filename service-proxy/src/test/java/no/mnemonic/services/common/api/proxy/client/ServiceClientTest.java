package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.proxy.TestException;
import no.mnemonic.services.common.api.proxy.TestService;
import no.mnemonic.services.common.api.proxy.Utils;
import no.mnemonic.services.common.api.proxy.messages.ServiceResponseMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;
import no.mnemonic.services.common.api.proxy.serializer.XStreamSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.hc.core5.http.HttpHeaders.CONTENT_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServiceClientTest {

  private static final String HTTP_LOCALHOST = "http://localhost";
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  public static final String APPLICATION_JSON = "application/json";
  public static final int MAX_STRING_LENGTH = 10_0000;

  private static WireMockServer SERVER;
  private ServiceV1HttpClient httpClient;
  private final Serializer serializer = serializer();

  @BeforeEach
  void setUp() {
    SERVER = new WireMockServer(wireMockConfig().dynamicPort());
    SERVER.start();

    SERVER.resetAll();

    httpClient = createAPIProvider();
  }

  @AfterEach
  void shutdown() {
    SERVER.shutdown();
  }

  @Test
  void testSimpleRequest() throws IOException, TestException {
    mockSingleResponse("getString", "response");
    assertEquals("response", proxy().getString("arg"));
    SERVER.verify(postRequestedFor(urlEqualTo("/service/v1/no.mnemonic.services.common.api.proxy.TestService/single/getString")));
  }

  @Test
  void testLargeResponse() throws IOException, TestException {
    String largeString = Stream.generate(() -> "a").limit(MAX_STRING_LENGTH /2).collect(Collectors.joining());
    mockSingleResponse("getString", largeString);
    assertEquals(largeString, proxy().getString("arg"));
  }

  @Test
  void testTooLargeResponse() throws IOException {
    String largeString = Stream.generate(() -> "a").limit(MAX_STRING_LENGTH * 2).collect(Collectors.joining());
    mockSingleResponse("getString", largeString);
    assertThrows(RuntimeException.class, ()->proxy().getString("arg"));
  }

  @Test
  void testResultSetRequest() throws IOException, TestException {
    mockResultSetResponse("getResultSet", 2, 5, 10, "response1", "response2");
    ResultSet<String> result = proxy().getResultSet("arg");
    assertEquals(ListUtils.list("response1", "response2"), ListUtils.list(result.iterator()));
    assertEquals(2, result.getCount());
    assertEquals(5, result.getLimit());
    assertEquals(10, result.getOffset());
    SERVER.verify(postRequestedFor(urlEqualTo("/service/v1/no.mnemonic.services.common.api.proxy.TestService/resultset/getResultSet")));
  }

  @Test
  void testHandleSimpleExceptionResponse() throws IOException {
    mockException("getString", new TestException("message", 25), false);
    TestException ex = assertThrows(TestException.class, () -> proxy().getString("arg"));
    assertEquals(25, ex.getCount());
  }

  @Test
  void testHandleResultSetExceptionResponse() throws IOException {
    mockException("getResultSet", new TestException("message", 25), true);
    TestException ex = assertThrows(TestException.class, () -> proxy().getResultSet("arg"));
    assertEquals(25, ex.getCount());
  }

  @Test
  void testHandleServerError() {
    stubServerError("getString", 500);
    assertThrows(IllegalStateException.class, () -> proxy().getString("arg"));
  }

  //helpers

  private void mockSingleResponse(String method, Object response) throws IOException {
    String json = MAPPER.writeValueAsString(
        ServiceResponseMessage.builder()
            .setResponse(
                serializer.serializeB64(response)
            )
            .build()
    );
    stubSingle(method, json, Utils.HTTP_OK_RESPONSE);
  }

  private void mockResultSetResponse(String method, int count, int limit, int offset, Object... responses) throws IOException {
    ObjectNode jsonResponse = MAPPER.createObjectNode()
        .put("count", count)
        .put("limit", limit)
        .put("offset", offset);
    ArrayNode data = MAPPER.createArrayNode();
    for (Object obj : responses) {
      data.add(serializer.serializeB64(obj));
    }
    jsonResponse.set("data", data);
    stubResultSet(method, MAPPER.writeValueAsString(jsonResponse), Utils.HTTP_OK_RESPONSE);
  }

  private void mockException(String method, Exception ex, boolean resultset) throws IOException {
    String json = MAPPER.writeValueAsString(
        ServiceResponseMessage.builder()
            .setException(
                serializer.serializeB64(ex)
            )
            .build()
    );
    if (resultset) {
      stubResultSet(method, json, Utils.HTTP_OK_RESPONSE);
    } else {
      stubSingle(method, json, Utils.HTTP_OK_RESPONSE);
    }
  }

  private static void stubSingle(String method, String json, int response) {
    SERVER.stubFor(
        post(urlEqualTo("/service/v1/no.mnemonic.services.common.api.proxy.TestService/single/" + method))
            .willReturn(
                aResponse()
                    .withStatus(response)
                    .withHeader(CONTENT_TYPE, APPLICATION_JSON)
                    .withBody(json)
            )
    );
  }

  private static void stubResultSet(String method, String json, int response) {
    SERVER.stubFor(
        post(urlEqualTo("/service/v1/no.mnemonic.services.common.api.proxy.TestService/resultset/" + method))
            .willReturn(
                aResponse()
                    .withStatus(response)
                    .withHeader(CONTENT_TYPE, APPLICATION_JSON)
                    .withBody(json)
            )
    );
  }

  private static void stubServerError(String method, int code) {
    SERVER.stubFor(
        post(urlEqualTo("/service/v1/no.mnemonic.services.common.api.proxy.TestService/single/" + method))
            .willReturn(
                aResponse()
                    .withStatus(code)
                    .withHeader(CONTENT_TYPE, APPLICATION_JSON)
            )
    );
  }

  private static ServiceV1HttpClient createAPIProvider() {
    return ServiceV1HttpClient.builder()
        .setDebugRequests(true)
        .setBaseURI(HTTP_LOCALHOST)
        .setBulkPort(SERVER.port())
        .setExpeditePort(SERVER.port())
        .setStandardPort(SERVER.port())
        .build();
  }

  private TestService proxy() {
    return proxyBuilder()
        .build()
        .getInstance();
  }

  private ServiceClient.ServiceClientBuilder<TestService> proxyBuilder() {
    return ServiceClient.<TestService>builder()
        .setProxyInterface(TestService.class)
        .setReadMaxStringLength(MAX_STRING_LENGTH)
        .setV1HttpClient(httpClient)
        .setSerializer(serializer);
  }

  private Serializer serializer() {
    return XStreamSerializer.builder()
        .setAllowedClass(TestService.class)
        .setAllowedClass(TestException.class)
        .build();
  }

}