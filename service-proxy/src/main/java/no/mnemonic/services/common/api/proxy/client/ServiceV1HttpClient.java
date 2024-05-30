package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.ServiceTimeOutException;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ConnectionRequestTimeoutException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.pool.PoolStats;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

@CustomLog
@Builder(buildMethodName = "_buildInternal", setterPrefix = "set")
public class ServiceV1HttpClient implements MetricAspect {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final int DEFAULT_BULK_PORT = 9001;
  private static final int DEFAULT_STANDARD_PORT = 9002;
  private static final int DEFAULT_EXPEDITE_PORT = 9003;
  private static final int DEFAULT_MAX_CONNECTIONS = 20;
  private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 180;
  private static final long DEFAULT_CONNECTION_WARN_AGE_LIMIT = 1000L * 60 * 3;

  //print debug of every request
  private final boolean debugRequests;
  //print list of all open requests on every getMetrics()
  private final boolean debugAllOpenRequests;

  @NonNull
  private final String baseURI;

  @Builder.Default
  private final int bulkPort = DEFAULT_BULK_PORT;
  @Builder.Default
  private final int standardPort = DEFAULT_STANDARD_PORT;
  @Builder.Default
  private final int expeditePort = DEFAULT_EXPEDITE_PORT;
  @Builder.Default
  private final int maxConnections = DEFAULT_MAX_CONNECTIONS;
  @Builder.Default
  private final long connectionTimeoutSeconds = DEFAULT_CONNECTION_TIMEOUT_SECONDS;
  //print warning for all open connections which are older than this limit
  @Builder.Default
  private final long connectionAgeWarningLimit = DEFAULT_CONNECTION_WARN_AGE_LIMIT;

  private static final String PACKAGE_VERSION = ServiceV1HttpClient.class.getPackage().getImplementationVersion();

  private final AtomicReference<PoolingHttpClientConnectionManager> connectionManager = new AtomicReference<>();
  private final AtomicReference<HttpClient> httpClient = new AtomicReference<>();
  private final Map<UUID, ServiceResponseContext> contextMap = new ConcurrentHashMap<>();
  private final Map<Integer, ServicePortContext> servicePortContextMap = new ConcurrentHashMap<>();
  private final LongAdder connectionAgeWarningsCounter = new LongAdder();

  private ServiceV1HttpClient init() {
    this.connectionManager.set(createConnectionManager());
    httpClient.set(
            HttpClientBuilder.create()
                    .setConnectionManager(connectionManager.get())
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setConnectionRequestTimeout(connectionTimeoutSeconds, TimeUnit.SECONDS)
                            .build())
                    .build()
    );
    return this;
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    PoolingHttpClientConnectionManager mgr = connectionManager.get();
    MetricsData data = new MetricsData();
    if (mgr != null) {
      PoolStats stats = mgr.getTotalStats();
      data.addData("available", stats.getAvailable());
      data.addData("leased", stats.getLeased());
      data.addData("max", stats.getMax());
      data.addData("pending", stats.getPending());
    }
    getContext(bulkPort).addMetrics(data);
    getContext(standardPort).addMetrics(data);
    getContext(expeditePort).addMetrics(data);

    if (debugAllOpenRequests) {
      contextMap.values().forEach(ctx -> LOGGER.info("Open response context: %s", ctx));
    }
    contextMap.values().stream()
            .filter(ctx -> ctx.isOlderThan(connectionAgeWarningLimit))
            .peek(ctx -> connectionAgeWarningsCounter.increment())
            .forEach(ctx -> LOGGER.warning("Possible connection leak: response context is older than %dms: %s", connectionAgeWarningLimit, ctx));
    data.addData("connection.age.warning.counter", connectionAgeWarningsCounter);

    return data;
  }

  /**
   * Perform HTTP request towards service proxy.
   *
   * @param service     the name of the service
   * @param method      the name of the method
   * @param type        the type of the expected response (single or resultset)
   * @param requestBody the request body (will be converted into a JSON object)
   * @return a response context. This response MUST be closed by the client using Closeable.close() or Resource.cancel()
   */
  public ServiceResponseContext request(String service, String method, ServiceRequestMessage.Type type, ServiceContext.Priority priority, ServiceRequestMessage requestBody) {
    try {
      String path = String.format("/service/v1/%s/%s/%s", service, type, method);
      int port = getPort(priority);
      URI uri = new URIBuilder(baseURI).setPort(port).setPath(path).build();
      HttpHost host = new HttpHost(uri.getScheme(), uri.getHost(), uri.getPort());
      HttpPost httpRequest = new HttpPost(uri);
      httpRequest.setEntity(new StringEntity(
              MAPPER.writeValueAsString(requestBody),
              ContentType.APPLICATION_JSON
      ));
      httpRequest.addHeader("Argus-Service-Client-Version", PACKAGE_VERSION);
      if (debugRequests) {
        LOGGER.debug("Invoking Service API %s", uri);
      }
      getContext(port).invocations.increment();
      ClassicHttpResponse response = httpClient.get().executeOpen(host, httpRequest, null);
      if (response.getCode() == 503) {
        response.close();
        getContext(port).serviceTimeouts.increment();
        throw new ServiceTimeOutException(response.getReasonPhrase(), service);
      } else if (response.getCode() == 504) {
        response.close();
        getContext(port).gatewayTimeouts.increment();
        throw new ServiceTimeOutException(response.getReasonPhrase(), service);
      } else if (response.getCode() >= 400) {
        response.close();
        throw new IllegalStateException(String.format("Unexpected response (%d) from HTTP proxy: %s", response.getCode(), response.getReasonPhrase()));
      } else {
        ServiceResponseContext ctx = ServiceResponseContext.builder()
                .setThreadName(Thread.currentThread().getName())
                .setTimestamp(System.currentTimeMillis())
                .setService(service)
                .setMethod(method)
                .setRequest(httpRequest)
                .setResponse(response)
                .setOnClose(contextMap::remove)
                .build();
        contextMap.put(ctx.getId(), ctx);
        return ctx;
      }
    } catch (ConnectionRequestTimeoutException e) {
      LOGGER.error(e, "Service request connection timeout");
      throw new ServiceTimeOutException(e.getMessage(), service);
    } catch (IOException | URISyntaxException e) {
      LOGGER.error(e, "Error invoking HTTP client");
      throw new IllegalStateException("Error invoking HTTP client", e);
    }
  }

  private ServicePortContext getContext(int bulkPort) {
    return servicePortContextMap.computeIfAbsent(bulkPort, ServicePortContext::new);
  }

  private PoolingHttpClientConnectionManager createConnectionManager() {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(maxConnections);
    connectionManager.setMaxTotal(maxConnections);
    return connectionManager;
  }

  private int getPort(ServiceContext.Priority priority) {
    switch (priority) {
      case bulk:
        return bulkPort;
      case standard:
        return standardPort;
      case expedite:
        return expeditePort;
      default:
        return standardPort;
    }
  }

  @RequiredArgsConstructor
  private static class ServicePortContext {
    private final int port;
    private final LongAdder invocations = new LongAdder();
    private final LongAdder gatewayTimeouts = new LongAdder();
    private final LongAdder serviceTimeouts = new LongAdder();

    public void addMetrics(MetricsData data) throws MetricException {
      data.addData("service.invocations." + port, invocations);
      data.addData("service.timeouts." + port, serviceTimeouts);
      data.addData("gateway.timeouts." + port, gatewayTimeouts);
    }
  }

  public static class ServiceV1HttpClientBuilder {
    public ServiceV1HttpClient build() {
      return _buildInternal().init();
    }
  }


}
