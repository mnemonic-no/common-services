package no.mnemonic.services.common.api.proxy.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.servlet.http.HttpServletResponse;
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
import no.mnemonic.services.common.api.ResultSetExtender;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import no.mnemonic.services.common.api.annotations.ResultSetExtention;
import no.mnemonic.services.common.api.proxy.ServiceProxyMetaDataContext;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import no.mnemonic.services.common.api.proxy.messages.ServiceResponseMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static no.mnemonic.services.common.api.proxy.Utils.HTTP_ERROR_RESPONSE;
import static no.mnemonic.services.common.api.proxy.Utils.HTTP_OK_RESPONSE;
import static no.mnemonic.services.common.api.proxy.Utils.toArgs;
import static no.mnemonic.services.common.api.proxy.Utils.toTypes;

/**
 * This handler deals with the actual invocation of methods on the proxied service,
 * and writing the responses back to the HTTP response
 * <p>
 * It has one method for handling single requests,
 * and another for handling streaming resultsets.
 * <p>
 * This handler will return HTTP code 200 on normal responses.
 * If <code>returnErrorResponses</code> is enabled, it will return 400 on checked exceptions
 * and 500 on unchecked exceptions. Else, all exception responses will also return 200.
 * Response 503 and 504 are used to detect gateway errors and service timeout.
 *
 * <b>NOTE</b> Enabling <code>returnErrorResponses</code> is a breaking change.
 * Make sure all clients are upgraded before enabling this!
 *
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
  @Builder.Default
  private final boolean returnErrorResponses = false;

  private final LongAdder totalRequestCount = new LongAdder();
  private final LongAdder totalKeepAliveCount = new LongAdder();
  private final LongAdder totalRequestTimeMillis = new LongAdder();
  private final LongAdder totalSimpleRequests = new LongAdder();
  private final LongAdder totalStreamingRequests = new LongAdder();
  private final Set<UUID> ongoingRequests = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Map<Class<?>, ResultSetExtender<?>> extenderFunctions = new HashMap<>();

  public int getOngoingRequestCount() {
    return ongoingRequests.size();
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
            .addData("total.request.count", totalRequestCount)
            .addData("total.request.streaming.count", totalStreamingRequests)
            .addData("total.request.simple.count", totalSimpleRequests)
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
    try (ServiceProxyMetaDataContext ignoredCtx = ServiceProxyMetaDataContext.initialize()) {
      totalSimpleRequests.increment();
      Method method = getMethod(methodName, request);
      handleMethodInvocation(method, request, httpResponse, this::writeSingleResponse);
    }
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
    try (ServiceProxyMetaDataContext ignoredCtx = ServiceProxyMetaDataContext.initialize()) {
      totalStreamingRequests.increment();
      Method method = getMethod(methodName, request);
      if (!ResultSet.class.isAssignableFrom(method.getReturnType())) {
        throw new IllegalStateException("Cannot write streaming for non-resultset returntype");
      }
      handleMethodInvocation(method, request, httpResponse, this::writeStreamingResponse);
    }
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
    ongoingRequests.add(requestID);

    try (TimerContext ignored = TimerContext.timerMillis(totalRequestTimeMillis::add);
         JsonGenerator generator = MAPPER.createGenerator(httpResponse.getOutputStream())
    ) {
      totalRequestCount.increment();
      try {
        if (LOGGER.isDebug()) {
          LOGGER.debug("<< invoke callID=%s method=%s priority=%s", requestID, method.getName(), request.getPriority());
        }
        set(debugListeners).forEach(l -> l.invocationStarted(requestID));
        try (
                AutoCloseable keepAliveTask = createKeepAliveTask(requestID, method, generator);
                ServiceSession ignoredSession = sessionFactory.openSession()
        ) {
          Object invocationResult = method.invoke(proxiedService, toArgs(serializer, request.getArguments()));
          //stop keepalive task before writing result back
          keepAliveTask.close();
          //return OK response
          httpResponse.setStatus(HTTP_OK_RESPONSE);
          //write the result back to writer
          writer.handle(method, requestID, serializer, generator, invocationResult);
        }
        set(debugListeners).forEach(l -> l.invocationSucceeded(requestID));
      } catch (InvocationTargetException e) {
        //since invocation is a method call, any exception is an InvocationTargetException
        if (returnErrorResponses) {
          if (e.getTargetException() instanceof RuntimeException) {
            //use 500 error for unchecked exceptions
            httpResponse.setStatus(HTTP_ERROR_RESPONSE);
          } else {
            //use 400 error for checked exceptions
            httpResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
          }
        } else {
          //legacy option, until all clients are upgraded
          httpResponse.setStatus(HTTP_OK_RESPONSE);
        }
        writeException(request.getRequestID(), serializer, generator, e);
        set(debugListeners).forEach(l -> l.invocationFailed(requestID));
      } catch (IOException e) {
        LOGGER.error(e, "Error writing response");
        set(debugListeners).forEach(l -> l.invocationFailed(requestID));
      } catch (Throwable e) {
        LOGGER.error(e, "Unexpected exception");
        set(debugListeners).forEach(l -> l.invocationFailed(requestID));
      }
    } finally {
      ongoingRequests.remove(requestID);
    }
  }

  /**
   * Resolve method to invoke
   */
  private Method getMethod(String methodName, ServiceRequestMessage request) throws NoSuchMethodException, ClassNotFoundException {
    return proxiedService.getClass().getMethod(methodName, toTypes(request.getArgumentTypes()));
  }

  /**
   * Create task in separate thread to write keepalives back to generator while the main thread  is executing the request
   */
  private AutoCloseable createKeepAliveTask(UUID requestID, Method method, JsonGenerator generator) {
    CountDownLatch closeLatch = new CountDownLatch(1);
    CountDownLatch finishedLatch = new CountDownLatch(1);
    executorService.submit(() -> {
      try {
        //keep doing keepalive until the latch is released
        while (!closeLatch.await(timeBetweenKeepAlives, TimeUnit.MILLISECONDS)) {
          if (LOGGER.isDebug()) {
            LOGGER.debug("<< keepalive callID=%s method=%s", requestID, method);
          }
          generator.writeRaw(" ");
          generator.flush();
          totalKeepAliveCount.increment();
          set(debugListeners).forEach(l -> l.keepAliveSent(requestID));
        }
      } catch (Exception e) {
        LOGGER.error(e, "Error sending keepAlive");
      } finally {
        //make sure to release finishedLatch to let the closeable result return on close()
        finishedLatch.countDown();
      }
    });
    return () -> {
      closeLatch.countDown();
      //wait for this task to end before returning, to ensure that the keepalive task stops sending keepalives before results are written
      tryTo(() -> finishedLatch.await(100, TimeUnit.MILLISECONDS));
    };
  }

  private boolean isUndeclaredException(Throwable e) {
    return e instanceof RuntimeException || e instanceof Error;
  }

  private void writeException(UUID requestID, Serializer serializer, JsonGenerator generator, InvocationTargetException invocationException) throws IOException {
    Throwable resultingException = invocationException.getTargetException();
    if (isUndeclaredException(resultingException)) {
      LOGGER.error(resultingException, "Caught undeclared exception");
      resultingException = new IllegalStateException("Caught undeclared exception: " + resultingException.getMessage());
    }
    generator.writeRaw(MAPPER.writeValueAsString(createException(requestID, serializer, (Exception) resultingException)));
  }

  private void writeSingleResponse(Method method, UUID requestID, Serializer serializer, JsonGenerator generator, Object invocationResult) throws IOException {
    generator.writeRaw(MAPPER.writeValueAsString(createSingleResponse(requestID, serializer, invocationResult)));
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< response callID=%s", requestID);
    }
  }

  private void writeStreamingResponse(Method method, UUID requestID, Serializer serializer, JsonGenerator generator, Object result) throws IOException {
    ResultSet<?> resultSet = (ResultSet<?>) result;
    Map<String, String> metaData = MapUtils.map(ServiceProxyMetaDataContext.getMetaData());

    if (!method.getReturnType().equals(ResultSet.class)) {
      ResultSetExtender extender = extenderFunctions.computeIfAbsent(method.getReturnType(), this::resolveExtenderFunction);
      if (extender != null) {
        metaData.putAll(MapUtils.map(extender.extract(resultSet)));
      }
    }

    generator.writeStartObject();
    generator.writeStringField("requestID", requestID.toString());
    if (!MapUtils.isEmpty(metaData)) {
      generator.writePOJOField("metaData", metaData);
    }
    if (resultSet != null) {
      try {
        generator.writeNumberField("count", resultSet.getCount());
        generator.writeNumberField("limit", resultSet.getLimit());
        generator.writeNumberField("offset", resultSet.getOffset());
        generator.writeArrayFieldStart("data");

        //write streaming result directly to output stream, to avoid memory buildup
        try {
          for (Object o : resultSet) {
            generator.writeString(
                    serializer.serializeB64(o)
            );
          }
        } finally {
          generator.writeEndArray();
        }
      } catch (Exception e) {
        //cancel resultset if abrupt failure
        LOGGER.error(e, "Error when writing ResultSet");
        resultSet.cancel();
      } finally {
        //close resultset
        resultSet.close();
      }
    }
    generator.writeEndObject();
  }

  private ResultSetExtender<?> resolveExtenderFunction(Class<?> declaredReturnType) {
    ResultSetExtention resultSetExtention = declaredReturnType.getAnnotation(ResultSetExtention.class);
    if (resultSetExtention == null) {
      return null;
    }
    try {
      return resultSetExtention.extender().getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new IllegalStateException("Declared resultset extention function could not be instantiated: " + resultSetExtention.extender(), e);
    }
  }

  interface ResponseWriter {
    void handle(Method method, UUID requestID, Serializer serializer, JsonGenerator generator, Object handle) throws IOException;
  }

  private ServiceResponseMessage createSingleResponse(UUID requestID, Serializer serializer, Object invocationResult) throws IOException {
    return ServiceResponseMessage.builder()
            .setMetaData(ServiceProxyMetaDataContext.getMetaData())
            .setRequestID(requestID)
            .setResponse(
                    serializer.serializeB64(invocationResult)
            )
            .build();
  }

  private ServiceResponseMessage createException(UUID requestID, Serializer serializer, Exception e) throws IOException {
    return ServiceResponseMessage.builder()
            .setMetaData(ServiceProxyMetaDataContext.getMetaData())
            .setRequestID(requestID)
            .setException(
                    serializer.serializeB64(e)
            )
            .build();
  }

  public interface DebugListener {
    void invocationStarted(UUID requestID);

    void invocationSucceeded(UUID requestID);

    void invocationFailed(UUID requestID);

    void keepAliveSent(UUID requestID);
  }

  public static class ServiceInvocationHandlerBuilder<T extends Service> {
    public ServiceInvocationHandlerBuilder<T> addSerializer(Serializer serializer) {
      this.serializers = MapUtils.addToMap(this.serializers, serializer.serializerID(), serializer);
      return this;
    }
  }
}
