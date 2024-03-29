package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.ServiceTimeOutException;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicReference;

@CustomLog
@Builder(buildMethodName = "_buildInternal", setterPrefix = "set")
public class ServiceV1HttpClient {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final int DEFAULT_BULK_PORT = 9001;
  private static final int DEFAULT_STANDARD_PORT = 9002;
  private static final int DEFAULT_EXPEDITE_PORT = 9003;
  private static final int DEFAULT_MAX_CONNECTIONS = 20;

  @NonNull
  private final String baseURI;
  private final boolean debug;

  @Builder.Default
  private final int bulkPort = DEFAULT_BULK_PORT;
  @Builder.Default
  private final int standardPort = DEFAULT_STANDARD_PORT;
  @Builder.Default
  private final int expeditePort = DEFAULT_EXPEDITE_PORT;
  @Builder.Default
  private final int maxConnections = DEFAULT_MAX_CONNECTIONS;

  private static final String PACKAGE_VERSION = ServiceV1HttpClient.class.getPackage().getImplementationVersion();

  private final AtomicReference<HttpClient> httpClient = new AtomicReference<>();

  private ServiceV1HttpClient init() {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(maxConnections);
    connectionManager.setMaxTotal(maxConnections);
    httpClient.set(HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
        .build()
    );
    return this;
  }

  /**
   * Perform HTTP request towards service proxy.
   *
   * @param service     the name of the service
   * @param method      the name of the method
   * @param type        the type of the expected response (single or resultset)
   * @param requestBody the request body (will be converted into a JSON object)
   * @return the HTTP response. This response MUST be closed by the client using Closeable.close()
   */
  public ClassicHttpResponse request(String service, String method, ServiceRequestMessage.Type type, ServiceContext.Priority priority, ServiceRequestMessage requestBody) {
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
      if (debug) {
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
        return response;
      }
    } catch (IOException | URISyntaxException e) {
      throw new IllegalStateException("Error invoking HTTP client", e);
    }
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
