package no.mnemonic.services.common.api.proxy.server;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.metrics.*;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.services.common.api.Service;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.MonitoredQueuedThreadPool;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.eclipse.jetty.server.Request.getLocalPort;
import static org.eclipse.jetty.server.Response.writeError;

/**
 * Produces a proxy instance, listening on the specified port.
 * The proxy will set up a servlet handling incoming requests on the format
 * <p>
 * /service/v1/&lt;SERVICE&gt;/single/&lt;METHOD&gt;
 * where SERVICE is the name of the service proxy, and METHOD is the name of the invoked method.
 * This endpoint expects POST methods with a JSONified ServiceRequestMessage.
 * The response is a JSONifed ServiceResponseMessage.
 * The endpoint will return HTTP code 200 on both normal and exceptional responses, and HTTP code 500 if the
 * service proxy itself fails.
 * <p>
 * For resultset (streaming results), there is a second endpoint
 * /service/v1/&lt;SERVICE&gt;/resultset/&lt;METHOD&gt;
 * This endpoint also expects POST methods with a JSONified ServiceRequestMessage.
 * However, the response will write a streaming JSON response with this exact form:
 * <code>
 * {
 * "count":5,
 * "limit":10,
 * "offset":20,
 * "data": [
 * "firstSerializedObject",
 * "secondSerializedObject",
 * ...
 * ]
 * }
 * </code>
 * This allows the client to decode a resultset from the initial fields, and read
 * the data fields in a streaming manner, dealing with each response individually, and closing the resultset when receiving end-of-array.
 * <p>
 * The resultset endpoint will return HTTP code 200 on both normal and exceptional responses, and HTTP code 500 if the endpoint itself fails.
 * Exceptional responses follow the same structure as for single methods.
 * <p>
 * The /v1 of the service proxy URL indicates that we may introduce breaking protocol changes later,
 * which would introduce a /v2 and /v3, etc. These changes must be done incrementally, keeping backwards compatibility for
 * any existing clients. The implementation of each protocol version should be split into separate code, both on
 * server and client side..
 */
@Builder(setterPrefix = "set")
@CustomLog
public class ServiceProxy implements LifecycleAspect, MetricAspect {

  private static final int DEFAULT_MAX_STRING_LENGTH = 50_000_000;
  private static final int DISABLE_CIRCUIT_BREAKER = -1;

  @Builder.Default
  private final int bulkPort = 9001;
  @Builder.Default
  private final int standardPort = 9002;
  @Builder.Default
  private final int expeditePort = 9003;
  @Builder.Default
  private final int bulkThreads = 5;
  @Builder.Default
  private final int standardThreads = 5;
  @Builder.Default
  private final int expediteThreads = 5;
  /**
   * Max size of request objects when parsing a service request.
   * Services should figure out other strategies for handling really large requests, such as fragmented upload.
   */
  @Builder.Default
  private final int readMaxStringLength = DEFAULT_MAX_STRING_LENGTH;
  /**
   * Optionally enable webserver circuit breaker to early reject requests when the thread pool´s idle thread count reaches or drops below the circuitBreakerLimit. Default is disabled.
   * If enabled, a request will be rejected if the thread pool has less idle threads than this limit (typical value is 1).
   */
  @Builder.Default
  private final int circuitBreakerLimit = DISABLE_CIRCUIT_BREAKER;

  @NonNull
  private final Map<Class<?>, ServiceInvocationHandler<?>> invocationHandlers;

  private final AtomicReference<Server> server = new AtomicReference<>();
  private final Map<Integer, MonitoredQueuedThreadPool> threadPools = new HashMap<>();
  private final Map<Integer, LongAdder> circuitBrokenCounters = new ConcurrentHashMap<>();

  @Override
  public void startComponent() {
    Server server = new Server();

    ServletHolder v1Servlet = new ServletHolder();
    v1Servlet.setServlet(
            ServiceV1Servlet.builder()
                    .setMapper(createMapper())
                    .setInvocationHandlers(MapUtils.map(invocationHandlers.entrySet(), e -> MapUtils.pair(e.getKey().getName(), e.getValue())))
                    .build()
    );

    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.addServlet(v1Servlet, "/service/v1/*");
    server.setHandler(contextHandler);

    threadPools.put(bulkPort, addConnector(server, bulkThreads, bulkPort));
    threadPools.put(standardPort, addConnector(server, standardThreads, standardPort));
    threadPools.put(expeditePort, addConnector(server, expediteThreads, expeditePort));

    //if circuit breaker is enabled, do not allow queueing of requests, instead immediately fail the request if the thread pool is (near) empty
    if (circuitBreakerLimit >= 0) {
      CircuitBreakerHandler circuitBreakerHandler = new CircuitBreakerHandler(threadPools, circuitBreakerLimit, circuitBrokenCounters);
      Handler handlerChain = new Handler.Sequence(circuitBreakerHandler, contextHandler);
      server.setHandler(handlerChain);
    }

    try {
      server.start();
      LOGGER.info("Listening...");
    } catch (Exception e) {
      LOGGER.error(e, "Error starting server");
      throw new IllegalStateException("Error starting server", e);
    }
    this.server.set(server);
  }

  @Override
  public Metrics getMetrics() {
    MetricsGroup poolMetrics = new MetricsGroup();
    this.threadPools.forEach((port, pool) -> {
      tryTo(
              () -> poolMetrics.addSubMetrics("port-" + port, createPoolMetrics(port, pool)),
              e -> LOGGER.warning(e, "Error adding metrics")
      );
    });
    return poolMetrics;
  }

  @Override
  public void stopComponent() {
    if (server.get() == null) return;
    try {
      LOGGER.info("Stopping server");
      server.get().stop();
    } catch (Exception e) {
      LOGGER.error(e, "Error stopping server");
      throw new IllegalStateException("Error stopping server", e);
    } finally {
      server.set(null);
    }
  }

  private MetricsData createPoolMetrics(Integer port, MonitoredQueuedThreadPool pool) throws MetricException {
    MetricsData data = new MetricsData();
    data.addData("max.task.latency", pool.getMaxTaskLatency());
    data.addData("avg.task.latency", pool.getAverageTaskLatency());
    data.addData("idle.threads", pool.getIdleThreads());
    data.addData("max.threads", pool.getMaxThreads());
    data.addData("max.busy.threads", pool.getMaxBusyThreads());
    data.addData("avg.queue.latency", pool.getAverageQueueLatency());
    data.addData("max.queue.latency", pool.getMaxQueueLatency());
    data.addData("max.queue.size", pool.getMaxQueueSize());
    data.addData("utilization.rate", pool.getUtilizationRate());
    data.addData("circuit.break.counter", ifNull(circuitBrokenCounters.get(port), 0L));
    return data;
  }

  private ObjectMapper createMapper() {
    ObjectMapper mapper = JsonMapper.builder().build();
    mapper.getFactory().setStreamReadConstraints(
            StreamReadConstraints.builder()
                    .maxStringLength(readMaxStringLength)
                    .build()
    );
    return mapper;
  }

  private MonitoredQueuedThreadPool addConnector(Server server, int threads, int port) {
    int acceptors = 5;
    int selectors = 5;
    int totalThreads = acceptors + selectors + threads;
    MonitoredQueuedThreadPool threadPool = new MonitoredQueuedThreadPool(totalThreads);
    ServerConnector connector = new ServerConnector(
            server,
            threadPool,
            null,
            null,
            acceptors,
            selectors,
            new HttpConnectionFactory(createHttpConfig())
    );

    connector.setPort(port);
    server.addConnector(connector);
    LOGGER.info("Adding connector on port %d", port);
    return threadPool;
  }

  private static HttpConfiguration createHttpConfig() {
    HttpConfiguration config = new HttpConfiguration();
    config.addCustomizer(createForwardCustomizer());
    return config;
  }

  private static ForwardedRequestCustomizer createForwardCustomizer() {
    //add a Forwarded request customizer which handles RFC7239 Forwarded header
    ForwardedRequestCustomizer customizer = new ForwardedRequestCustomizer();
    customizer.setForwardedOnly(true);
    return customizer;
  }

  public static class CircuitBreakerHandler extends Handler.Abstract {

    private final Map<Integer, MonitoredQueuedThreadPool> threadPools;
    private final int threshold;
    private final Map<Integer, LongAdder> metrics;

    CircuitBreakerHandler(Map<Integer, MonitoredQueuedThreadPool> threadPools, int threshold, Map<Integer, LongAdder> metrics) {
      this.threadPools = threadPools;
      this.threshold = threshold;
      this.metrics = metrics;
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) {
      QueuedThreadPool threadPool = this.threadPools.get(getLocalPort(request));
      if (threadPool != null && threadPool.getIdleThreads() <= threshold) {
        LOGGER.warning("Rejecting request, server too busy");
        writeError(request, response, callback, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Server too busy");
        ifNotNullDo(metrics.computeIfAbsent(getLocalPort(request), p -> new LongAdder()), LongAdder::increment);
        return true;
      } else {
        return false;
      }
    }
  }

  public static class ServiceProxyBuilder {
    public <T extends Service> ServiceProxyBuilder addInvocationHandler(Class<T> type, ServiceInvocationHandler<T> handler) {
      this.invocationHandlers = MapUtils.addToMap(this.invocationHandlers, type, handler);
      return this;
    }
  }

}
