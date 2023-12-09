package no.mnemonic.services.common.api.proxy.client;

import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.annotations.ResultSetExtention;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Invocation handler which converts the request to a request message,
 * sends requestmessage to the correct proxy endpoint,
 * receives the response, and delegates to the correct response handler.
 *
 * @param <T> the service type
 */
@Builder(setterPrefix = "set")
@CustomLog
class ClientV1InvocationHandler<T extends Service> implements InvocationHandler, MetricAspect {

  private static final String OBJECT_EQUALS = "equals";
  private static final String OBJECT_HASH_CODE = "hashCode";
  private static final String OBJECT_TO_STRING = "toString";
  private static final String SERVICE_PROXY_GET_SERVICE_CONTEXT = "getServiceContext";

  @NonNull
  private final ServiceContext serviceContext;
  @NonNull
  private final Class<T> proxyInterface;
  @NonNull
  private final ServiceV1HttpClient httpClient;
  @NonNull
  private final Supplier<ServiceContext.Priority> priority;
  @NonNull
  private final Serializer serializer;
  @NonNull
  private final LongAdder requests;
  @NonNull
  private final LongAdder totalRequestTime;
  @NonNull
  private final Map<Class<?>, Function<ResultSet<?>, ? extends ResultSet<?>>> extenderFunctions;

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
            .addData("requests", requests)
            .addData("total.request.time.ms", totalRequestTime);
  }

  @Override
  public Object invoke(Object object, Method method, Object[] arguments) throws Throwable {
    if (method.getName().equals(OBJECT_HASH_CODE)) {
      return proxyInterface.hashCode();
    }
    if (method.getName().equals(OBJECT_TO_STRING)) {
      return proxyInterface.toString();
    }
    if (method.getName().equals(OBJECT_EQUALS)) {
      return arguments[0] == object;
    }
    if (method.getName().equals(SERVICE_PROXY_GET_SERVICE_CONTEXT)) {
      return serviceContext;
    }
    // try invoking
    requests.increment();
    try (TimerContext ignored = TimerContext.timerMillis(totalRequestTime::add)) {
      return invoke(method, arguments);
    }
  }

  //private methods

  private Object invoke(Method method, Object[] arguments) throws Exception {
    //noinspection unused

    ServiceMessageConverter serviceMessageConverter = new ServiceMessageConverter(serializer);

    UUID requestID = UUID.randomUUID();
    ServiceRequestMessage request = serviceMessageConverter.convert(requestID, method, arguments, priority.get());

    if (LOGGER.isDebug()) {
      LOGGER.debug(">> request [callID=%s service=%s method=%s arguments=%s]",
              request.getRequestID(),
              proxyInterface.getSimpleName(),
              method.getName(),
              request.getArgumentTypes()
      );
    }

    if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
      //noinspection resource handled by ResultSetParser
      ClassicHttpResponse response = httpClient.request(
              proxyInterface.getName(),
              method.getName(),
              ServiceRequestMessage.Type.resultset,
              request.getPriority(),
              request
      );
      return handleResultSet(method, response.getEntity().getContent());
    } else {
      try (ClassicHttpResponse response = httpClient.request(
              proxyInterface.getName(),
              method.getName(),
              ServiceRequestMessage.Type.single,
              request.getPriority(),
              request
      )) {
        return serviceMessageConverter.readResponseMessage(response.getEntity().getContent());
      }
    }

  }

  private <R> ResultSet<R> handleResultSet(Method invokedMethod, InputStream response) throws Exception {
    if (invokedMethod.getReturnType().equals(ResultSet.class)) {
      return new ResultSetParser(serializer).parse(response);
    } else {
      //if the declared method returns a subclass of ResultSet, we may need an extender function
      Function<ResultSet<?>, ? extends ResultSet<?>> extender = extenderFunctions.computeIfAbsent(invokedMethod.getReturnType(), this::resolveExtenderFunction);
      //extend the declared return type to the correct subclass
      //noinspection unchecked
      return (ResultSet<R>) extender.apply(new ResultSetParser(serializer).parse(response));
    }
  }

  private Function<ResultSet<?>, ? extends ResultSet<?>> resolveExtenderFunction(Class<?> declaredReturnType) {
    ResultSetExtention resultSetExtention = declaredReturnType.getAnnotation(ResultSetExtention.class);
    if (resultSetExtention == null) {
      throw new IllegalStateException("Declared returntype of invoked method is a subclass of ResultSet, but no extender function is defined for this type: " + declaredReturnType);
    }
    try {
      return resultSetExtention.extender().getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new IllegalStateException("Declared resultset extention function could not be instantiated: " + resultSetExtention.extender(), e);
    }
  }

}
