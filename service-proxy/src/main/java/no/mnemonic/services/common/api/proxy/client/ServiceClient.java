package no.mnemonic.services.common.api.proxy.client;

import lombok.Builder;
import lombok.NonNull;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsGroup;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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

  @NonNull
  private final Class<T> proxyInterface;
  @NonNull
  private final ServiceV1HttpClient v1HttpClient;
  @Builder.Default
  private final ServiceContext.Priority defaultPriority = ServiceContext.Priority.standard;
  @NonNull
  private final Serializer serializer;
  @NonNull
  private final Map<Class<?>, Function<ResultSet<?>, ? extends ResultSet<?>>> extenderFunctions;

  private final ThreadLocal<ServiceContext.Priority> threadPriority = new ThreadLocal<>();
  private final ThreadLocal<ServiceContext.Priority> nextPriority = new ThreadLocal<>();
  private static final ThreadLocal<ServiceContext.Priority> globalThreadPriority = new ThreadLocal<>();

  private final AtomicReference<T> proxy = new AtomicReference<>();
  private final AtomicReference<ClientV1InvocationHandler<T>> handler = new AtomicReference<>();

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsGroup group = new MetricsGroup();
    if (handler.get() != null) {
      group.addSubMetrics("handler", handler.get().getMetrics());
    }
    return group;
  }

  public T getInstance() {
    return proxy.updateAndGet(p -> {
      if (p == null) p = createProxy();
      return p;
    });
  }

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
        .build();
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

  public static class ServiceClientBuilder<T extends Service> {
    public ServiceClientBuilder() {
      this.extenderFunctions = new HashMap<>();
    }

    public <R extends ResultSet<?>> ServiceClientBuilder<T> withExtenderFunction(Class<R> returnType, Function<ResultSet<?>, R> extenderFunction) {
      this.extenderFunctions.put(returnType, extenderFunction);
      return this;
    }
  }

}
