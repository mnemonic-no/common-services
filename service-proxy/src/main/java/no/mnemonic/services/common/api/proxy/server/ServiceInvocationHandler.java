package no.mnemonic.services.common.api.proxy.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.Singular;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import no.mnemonic.services.common.api.proxy.messages.ServiceResponseMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.services.common.api.proxy.Utils.HTTP_OK_RESPONSE;
import static no.mnemonic.services.common.api.proxy.Utils.toArgs;
import static no.mnemonic.services.common.api.proxy.Utils.toTypes;

/**
 * This handler deals with the actual invocation of methods on the proxied service,
 * and writing the responses back to the HTTP response
 * <p>
 * It has one method for handling single requests,
 * and another for handling streaming resultsets..
 */
@Builder(setterPrefix = "set")
@CustomLog
public class ServiceInvocationHandler<T extends Service> implements MetricAspect {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final int DEFAULT_TIME_BETWEEN_KEEPALIVES = 1000;

  @NonNull
  private final T proxiedService;
  @NonNull
  private final Map<String, Serializer> serializers;
  @NonNull
  private final ExecutorService executorService;
  @Singular
  private final Set<DebugListener> debugListeners;
  @NonNull
  private final ServiceSessionFactory sessionFactory;
  @Builder.Default
  private long timeBetweenKeepAlives = DEFAULT_TIME_BETWEEN_KEEPALIVES;

  private final LongAdder totalRequestCount = new LongAdder();
  private final LongAdder totalKeepAliveCount = new LongAdder();
  private final LongAdder totalRequestTimeMillis = new LongAdder();

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
        .addData("total.request.count", totalRequestCount)
        .addData("total.keepalive.count", totalKeepAliveCount)
        .addData("total.request.time.ms", totalRequestTimeMillis);
  }

  /**
   * Handle a single method invocation
   *
   * @param methodName   method name
   * @param request      the invocation request
   * @param httpResponse the HTTP response to write the result back to
   */
  public void handleSingle(
      String methodName,
      @NonNull ServiceRequestMessage request,
      HttpServletResponse httpResponse
  ) throws Exception {
    Method method = getMethod(methodName, request);
    handleMethodInvocation(method, request, httpResponse, this::writeSingleResponse);
  }

  /**
   * Handle a method invocation with a resultset response,
   * to deal with streaming resultsets.
   *
   * @param methodName   method name
   * @param request      the invocation request
   * @param httpResponse the HTTP response to write the result back to
   */
  public void handleStreaming(
      String methodName,
      @NonNull ServiceRequestMessage request,
      HttpServletResponse httpResponse
  ) throws Exception {
    Method method = getMethod(methodName, request);
    if (!ResultSet.class.isAssignableFrom(method.getReturnType())) {
      throw new IllegalStateException("Cannot write streaming for non-resultset returntype");
    }
    handleMethodInvocation(method, request, httpResponse, this::writeStreamingResponse);
  }

  //private methods

  private void handleMethodInvocation(Method method,
                                      ServiceRequestMessage request,
                                      HttpServletResponse httpResponse,
                                      ResponseWriter writer
  ) throws Exception {
    Serializer serializer = serializers.get(request.getSerializerID());
    if (serializer == null) {
      throw new IllegalArgumentException("Unknown serializer: " + request.getSerializerID());
    }
    UUID requestID = ifNull(request.getRequestID(), UUID::randomUUID);
    //always OK response (exceptions from the service are also 200 OK)
    httpResponse.setStatus(HTTP_OK_RESPONSE);
    try (TimerContext ignored = TimerContext.timerMillis(totalRequestTimeMillis::add);
         ServiceSession session = sessionFactory.openSession();
         JsonGenerator generator = MAPPER.createGenerator(httpResponse.getOutputStream())
    ) {
      totalRequestCount.increment();
      try {
        //send keepalives while invoking the method
        if (LOGGER.isDebug()) {
          LOGGER.debug("<< invoke callID=%s method=%s priority=%s", requestID, method.getName(), request.getPriority());
        }
        Object invocationResult = invokeWhileSendingKeepalive(requestID, method, request.getArguments(), serializer, generator);
        //write the result back
        writer.handle(requestID, serializer, generator, invocationResult);
      } catch (ExecutionException e) {
        //since invocation is a method call, any exception is an InvocationTargetException
        assert (e.getCause() instanceof InvocationTargetException);
        writeException(request.getRequestID(), serializer, generator, (InvocationTargetException) e.getCause());
      } catch (Throwable e) {
        LOGGER.error(e, "Unexpected exception");
      }
    }
  }

  /**
   * Resolve method to invoke
   */
  private Method getMethod(String methodName, ServiceRequestMessage request) throws NoSuchMethodException, ClassNotFoundException {
    return proxiedService.getClass().getMethod(methodName, toTypes(request.getArgumentTypes()));
  }

  /**
   * Invoke method with arguments, while generating keepalive bytes
   */
  private Object invokeWhileSendingKeepalive(UUID requestID, Method method, List<String> arguments, Serializer serializer, JsonGenerator generator) throws IOException, ExecutionException, InterruptedException {
    Future<Object> future = executorService.submit(() -> method.invoke(proxiedService, toArgs(serializer, arguments)));
    while (true) {
      try {
        return future.get(timeBetweenKeepAlives, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        //keepalive
        generator.writeRaw(" ");
        totalKeepAliveCount.increment();
        if (LOGGER.isDebug()) {
          LOGGER.debug("<< keepalive callID=%s method=%s", requestID, method);
        }
        set(debugListeners).forEach(l -> l.keepAliveSent(requestID));
      }
    }
  }

  private boolean isUndeclaredException(Throwable e) {
    return e instanceof RuntimeException || e instanceof Error;
  }

  private void writeException(UUID requestID, Serializer serializer, JsonGenerator generator, InvocationTargetException invocationException) throws IOException {
    Throwable resultingException = invocationException.getTargetException();
    if (isUndeclaredException(resultingException)) {
      resultingException = new IllegalStateException("Caught undeclared exception: " + resultingException.getMessage());
    }
    generator.writeRaw(MAPPER.writeValueAsString(createException(requestID, serializer, (Exception) resultingException)));
  }

  private void writeSingleResponse(UUID requestID, Serializer serializer, JsonGenerator generator, Object invocationResult) throws IOException {
    generator.writeRaw(MAPPER.writeValueAsString(createSingleResponse(requestID, serializer, invocationResult)));
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< response callID=%s", requestID);
    }
  }

  private void writeStreamingResponse(UUID requestID, Serializer serializer, JsonGenerator generator, Object result) throws IOException {
    ResultSet<?> resultSet = (ResultSet<?>) result;
    generator.writeStartObject();
    generator.writeStringField("requestID", requestID.toString());
    if (resultSet != null) {
      generator.writeNumberField("count", resultSet.getCount());
      generator.writeNumberField("limit", resultSet.getLimit());
      generator.writeNumberField("offset", resultSet.getOffset());
      generator.writeArrayFieldStart("data");

      //write streaming result directly to output stream, to avoid memory buildup
      for (Object o : resultSet) {
        generator.writeString(
                serializer.serializeB64(o)
        );
      }
      //close resultset
      generator.writeEndArray();
    }
    generator.writeEndObject();
  }

  interface ResponseWriter {
    void handle(UUID requestID, Serializer serializer, JsonGenerator generator, Object handle) throws IOException;
  }

  private ServiceResponseMessage createSingleResponse(UUID requestID, Serializer serializer, Object invocationResult) throws IOException {
    return ServiceResponseMessage.builder()
        .setRequestID(requestID)
        .setResponse(
            serializer.serializeB64(invocationResult)
        )
        .build();
  }

  private ServiceResponseMessage createException(UUID requestID, Serializer serializer, Exception e) throws IOException {
    return ServiceResponseMessage.builder()
        .setRequestID(requestID)
        .setException(
            serializer.serializeB64(e)
        )
        .build();
  }

  public interface DebugListener {
    void keepAliveSent(UUID requestID);
  }

  public static class ServiceInvocationHandlerBuilder<T extends Service> {
    public ServiceInvocationHandlerBuilder<T> addSerializer(Serializer serializer) {
      this.serializers = MapUtils.addToMap(this.serializers, serializer.serializerID(), serializer);
      return this;
    }
  }
}
