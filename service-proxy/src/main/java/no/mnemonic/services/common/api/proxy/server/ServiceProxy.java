package no.mnemonic.services.common.api.proxy.server;


import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.services.common.api.Service;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Produces a proxy instance, listening on the specified port.
 * The proxy will set up a servlet handling incoming requests on the format
 *
 * /service/v1/&lt;SERVICE&gt;/single/&lt;METHOD&gt;
 * where SERVICE is the name of the service proxy, and METHOD is the name of the invoked method.
 * This endpoint expects POST methods with a JSONified ServiceRequestMessage.
 * The response is a JSONifed ServiceResponseMessage.
 * The endpoint will return HTTP code 200 on both normal and exceptional responses, and HTTP code 500 if the
 * service proxy itself fails.
 *
 * For resultset (streaming results), there is a second endpoint
 * /service/v1/&lt;SERVICE&gt;/resultset/&lt;METHOD&gt;
 * This endpoint also expects POST methods with a JSONified ServiceRequestMessage.
 * However, the response will write a streaming JSON response with this exact form:
 * <code>
 *   {
 *     "count":5,
 *     "limit":10,
 *     "offset":20,
 *     "data": [
 *      "firstSerializedObject",
 *      "secondSerializedObject",
 *      ...
 *     ]
 *   }
 * </code>
 * This allows the client to decode a resultset from the initial fields, and read
 * the data fields in a streaming manner, dealing with each response individually, and closing the resultset when receiving end-of-array.
 *
 * The resultset endpoint will return HTTP code 200 on both normal and exceptional responses, and HTTP code 500 if the endpoint itself fails.
 * Exceptional responses follow the same structure as for single methods.
 *
 * The /v1 of the service proxy URL indicates that we may introduce breaking protocol changes later,
 * which would introduce a /v2 and /v3, etc. These changes must be done incrementally, keeping backwards compatibility for
 * any existing clients. The implementation of each protocol version should be split into separate code, both on
 * server and client side..
 *
 */
@Builder(setterPrefix = "set")
@CustomLog
public class ServiceProxy implements LifecycleAspect {

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

  @NonNull
  private final Map<Class<?>, ServiceInvocationHandler<?>> invocationHandlers;

  private final AtomicReference<Server> server = new AtomicReference<>();

  @Override
  public void startComponent() {
    Server server = new Server();
    ServletContextHandler contextHandler = new ServletContextHandler();

    ServletHolder v1Servlet = new ServletHolder();
    v1Servlet.setServlet(
        ServiceV1Servlet.builder()
            .setInvocationHandlers(MapUtils.map(invocationHandlers.entrySet(), e->MapUtils.pair(e.getKey().getName(), e.getValue())))
            .build()
    );
    contextHandler.addServlet(v1Servlet, "/service/v1/*");

    server.setHandler(contextHandler);

    addConnector(server, bulkThreads, bulkPort);
    addConnector(server, standardThreads, standardPort);
    addConnector(server, expediteThreads, expeditePort);

    try {
      server.start();
    } catch (Exception e) {
      LOGGER.error(e, "Error starting server");
      throw new IllegalStateException("Error starting server", e);
    }
    this.server.set(server);
  }

  @Override
  public void stopComponent() {
    if (server.get() == null) return;
    try {
      server.get().stop();
    } catch (Exception e) {
      LOGGER.error(e, "Error stopping server");
      throw new IllegalStateException("Error stopping server", e);
    } finally {
      server.set(null);
    }
  }

  private void addConnector(Server server, int threads, int port) {
    int acceptors = 5;
    int selectors = 5;
    int totalThreads = acceptors + selectors + threads;
    ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
    ServerConnector connector = new ServerConnector(server, executor, null, null, acceptors, selectors, new HttpConnectionFactory());
    connector.setPort(port);
    server.addConnector(connector);
    LOGGER.info("Adding connector on port %d", port);
  }

  public static class ServiceProxyBuilder {
    public <T extends Service> ServiceProxyBuilder addInvocationHandler(Class<T> type, ServiceInvocationHandler<T> handler) {
      this.invocationHandlers = MapUtils.addToMap(this.invocationHandlers, type, handler);
      return this;
    }
  }

}
