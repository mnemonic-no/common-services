package no.mnemonic.services.common.api.proxy;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * A simple threadlocal utility to expose request metadata during execution, which has been
 * picked up by the ServiceProxy.
 * <p>
 * The context is automatically set up for every invocation executed by the ServiceInvocationHandler.
 * The service implementation should pick up any data from the request context as needed.
 * <p>
 * To pick up the real clientIP of the current request:
 * <code>
 *    ServiceProxyMetaDataContext.get().getClientIP();
 * </code>
 * <p>
 * To pick up the HTTP headers of the SPI request:
 * <code>
 *    ServiceProxyMetaDataContext.get().getHeaders();
 * </code>
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class ServiceProxyRequestContext implements Closeable {

  private static final ThreadLocal<ServiceProxyRequestContext> CONTEXT = new ThreadLocal<>();

  private final String clientIP;
  private final Map<String, List<String>> headers;

  public static ServiceProxyRequestContext initialize(String clientIP, Map<String, List<String>> headers) {
    ServiceProxyRequestContext ctx = new ServiceProxyRequestContext(clientIP, headers);
    CONTEXT.set(ctx);
    return ctx;
  }

  public static boolean isSet() {
    return CONTEXT.get() != null;
  }

  /**
   *
   * @return the currently initialized context.
   * @throws IllegalStateException if the context is not set
   */
  public static ServiceProxyRequestContext get() {
    if (!isSet()) throw new IllegalStateException("ServiceProxyRequestContext not set");
    return CONTEXT.get();
  }

  @Override
  public void close() {
    CONTEXT.remove();
  }

}
