package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.NonNull;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsGroup;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetExtender;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.services.common.api.ServiceContext.Priority.standard;

/**
 * Service Client which provides a client-side proxy to the targeted Service interface.
 * The client will intercept all service invocation methods,
 * submit a request to the service proxy endpoint,
 * handle the results and return the response when done.
 *
 * All expected response classes must be permitted by the provided Serializer.
 *
 * The client will detect checked exceptions from the service, and re-throw them
 * (as long as the exception class is permitted by the serializer).
 *
 */
@Builder(setterPrefix = "set")
public class ServiceClient<T extends Service> implements MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(ServiceClient.class);
  private static final int DEFAULT_MAX_STRING_LENGTH = 50_000_000;

  @NonNull
  private final Class<T> proxyInterface;
  @NonNull
  private final ServiceV1HttpClient v1HttpClient;
  @Builder.Default
  private final ServiceContext.Priority defaultPriority = ServiceContext.Priority.standard;
  @NonNull
  private final Serializer serializer;
  @NonNull
  private final Map<Class<?>, ResultSetExtender<?>> extenderFunctions;
  @Builder.Default
  private final List<ServiceClientMetaDataHandler> metaDataHandlers = new ArrayList<>();

  @Builder.Default
  private final int readMaxStringLength = DEFAULT_MAX_STRING_LENGTH;

  private final ThreadLocal<ServiceContext.Priority> threadPriority = new ThreadLocal<>();
  private final ThreadLocal<ServiceContext.Priority> nextPriority = new ThreadLocal<>();
  private static final ThreadLocal<ServiceContext.Priority> globalThreadPriority = new ThreadLocal<>();

  private final AtomicReference<T> proxy = new AtomicReference<>();
  private final AtomicReference<ClientV1InvocationHandler<T>> handler = new AtomicReference<>();

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsGroup group = new MetricsGroup();
    group.addSubMetrics("serializer", serializer.getMetrics());
    if (handler.get() != null) {
      group.addSubMetrics("handler", handler.get().getMetrics());
    }
    return group;
  }

  /**
   * Usually, the HTTP client for ResultSet responses from the ServiceClient proxy are
   * kept open until the ResultSet is iterated until closure, or must be closed by the client.
   * Therefore, the resource may not be properly closed.
   *
   * Invoke this method when thread is done with all requests, to ensure that any dangling thread resources
   * (which have not been closed already) are closed.
   */
  public static void closeThreadResources() {
    ClientV1InvocationHandler.closeThreadResources();
  }

  /**
   * @return the ServiceClient proxy instance for the configured interface.
   * Invoking methods on the returned object will invoke the service client proxy.
   */
  public T getInstance() {
    return proxy.updateAndGet(p -> {
      if (p == null) p = createProxy();
      return p;
    });
  }

  //private methods

  private T createProxy() {
    //noinspection unchecked
    return (T) Proxy.newProxyInstance(
        proxyInterface.getClassLoader(),
        new Class[]{proxyInterface},
        getHandler()
    );
  }

  private ClientV1InvocationHandler<T> getHandler() {
    return handler.updateAndGet(existing -> {
      if (existing != null) return existing;
      return createHandler();
    });
  }

  private ClientV1InvocationHandler<T> createHandler() {
    return ClientV1InvocationHandler.<T>builder()
        .setPriority(this::determinePriority)
        .setServiceContext(new ServiceContextImpl())
        .setHttpClient(v1HttpClient)
        .setProxyInterface(proxyInterface)
        .setSerializer(serializer)
        .setExtenderFunctions(extenderFunctions)
        .setMetaDataHandlers(metaDataHandlers)
        .setMapper(createMapper())
        .build();
  }

  private ObjectMapper createMapper() {
    ObjectMapper mapper = JsonMapper.builder().build();
    mapper.getFactory().setStreamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(readMaxStringLength).build()
    );
    return mapper;
  }

  private class ServiceContextImpl implements ServiceContext {
    @Override
    public void setThreadPriority(Priority priority) {
      ServiceClient.this.threadPriority.set(priority);
    }

    @Override
    public void setNextRequestPriority(Priority priority) {
      ServiceClient.this.nextPriority.set(priority);
    }
  }

  private ServiceContext.Priority determinePriority() {
    if (nextPriority.get() != null) {
      ServiceContext.Priority priority = nextPriority.get();
      nextPriority.remove();
      return priority;
    } else if (threadPriority.get() != null) {
      return threadPriority.get();
    } else if (globalThreadPriority.get() != null) {
      return globalThreadPriority.get();
    } else {
      return defaultPriority;
    }
  }

  /**
   * Allow setting the priority for ALL ServiceMessageClients in this VM.
   * This priority will override the default priority set on the client, but will be overridden by
   * the threadPriority or the nextPriority if set on any specific client.
   *
   * @param priority the priority to use for the current thread, if not overridden by threadPriority or nextPriority
   */
  public static void setGlobalThreadPriority(ServiceContext.Priority priority) {
    globalThreadPriority.set(priority);
  }

  /**
   * Simplified method to allow scoped global thread priority, e.g.
   * <code>
   *   try (ThreadPriorityContext ctx = ServiceClient.createGlobalThreadPriorityContext(Priority.bulk) {
   *     //do some bulk priority code
   *   } //this auto-closes the context
   * </code>
   * @param priority the priority to use for all ServiceClient invocations in this scope
   * @return an auto-closeable context scope
   */
  public static ServiceContext.ThreadPriorityContext createGlobalThreadPriorityContext(ServiceContext.Priority priority) {
    setGlobalThreadPriority(priority);
    return () -> setGlobalThreadPriority(null);
  }

  public static ServiceContext.Priority getGlobalThreadPriority() {
    return ifNull(globalThreadPriority.get(), standard);
  }

  public static class ServiceClientBuilder<T extends Service> {
    public ServiceClientBuilder() {
      this.extenderFunctions = new HashMap<>();
    }

    /**
     * Register an resultset extender implementation
     * @param returnType the type of resultset to extend to
     * @param extenderFunction the extenderfunction to use to convert the standard resultset to the returntype
     */
    public <R extends ResultSet<?>> ServiceClientBuilder<T> withExtender(Class<R> returnType, ResultSetExtender<R> extenderFunction) {
      this.extenderFunctions.put(returnType, extenderFunction);
      return this;
    }

    /**
     * Register a metadata handler which will be invoked with metadata returned to the client
     * @param handler handler implementation
     */
    public ServiceClientBuilder<T> withMetaDataHandler(ServiceClientMetaDataHandler handler) {
      this.metaDataHandlers$set = true;
      this.metaDataHandlers$value = ListUtils.addToList(this.metaDataHandlers$value, handler);
      return this;
    }
  }

}
