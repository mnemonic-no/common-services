package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.utilities.collections.SetUtils;
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

@CustomLog
@Builder(buildMethodName = "_buildInternal", setterPrefix = "set")
public class ServiceV1HttpClient implements MetricAspect {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final int DEFAULT_BULK_PORT = 9001;
  private static final int DEFAULT_STANDARD_PORT = 9002;
  private static final int DEFAULT_EXPEDITE_PORT = 9003;
  private static final int DEFAULT_MAX_CONNECTIONS = 20;
  private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 180;

  //print debug of every request
  private final boolean debugRequests;
  //print list of open requests on every getMetrics()
  private final boolean debugOpenRequests;

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


  private static final String PACKAGE_VERSION = ServiceV1HttpClient.class.getPackage().getImplementationVersion();

  private final AtomicReference<PoolingHttpClientConnectionManager> connectionManager = new AtomicReference<>();
  private final AtomicReference<HttpClient> httpClient = new AtomicReference<>();
  private final Map<UUID, ServiceResponseContext> contextMap = new ConcurrentHashMap<>();

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
    if (debugOpenRequests) {
      contextMap.forEach((k,ctx) -> LOGGER.info("Open response context: %s", ctx));
    }
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
      URI uri = new URIBuilder(baseURI).setPort(getPort(priority)).setPath(path).build();
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
      ClassicHttpResponse response = httpClient.get().executeOpen(host, httpRequest, null);
      if (SetUtils.in(response.getCode(), 503, 504)) {
        response.close();
        throw new ServiceTimeOutException(response.getReasonPhrase(), service);
      } else if (response.getCode() >= 400) {
        response.close();
        throw new IllegalStateException(String.format("Unexpected response (%d) from HTTP proxy: %s", response.getCode(), response.getReasonPhrase()));
      } else {
        ServiceResponseContext ctx = new ServiceResponseContext(service, method, httpRequest, response, contextMap::remove);
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

  public static class ServiceV1HttpClientBuilder {
    public ServiceV1HttpClient build() {
      return _buildInternal().init();
    }
  }


}
