package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.MessagingInterruptedException;
import no.mnemonic.messaging.requestsink.RequestHandler;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetStreamInterruptedException;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.ServiceTimeOutException;
import no.mnemonic.services.common.api.annotations.ResultSetExtention;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNull;
import static no.mnemonic.services.common.api.ServiceContext.Priority.standard;

/**
 * A Service Message Client acts as a factory, providing an interface implementation which
 * proxies requests across a RequestSink infrastructure.
 * <p>
 * This requires a requestSink which is attached to a ServiceMessageHandler on the other end.
 * <p>
 * <b>Sending a request</b>
 * <p>
 * The ServiceMessageClient&lt;SERVICE&gt; exposes the interface SERVICE using <code>getInstance()</code>.
 * Invoking any method on the <code>SERVICE</code> instance will send a request message.
 * The current thread will block until the response is received and returned, or until timeout.
 * <p>
 * <b>Prioritizing requests</b>
 * <p>
 * To prioritize requests, the client can choose between three approaches:
 * <ol>
 *   <li>Change the <code>defaultPriority</code> of this ServiceMessageClient</li>
 *   <li>Change the <i>thread priority</i> of the current thread by invoking <code>((ServiceProxy)SERVICE).getServiceContext().setThreadPriority(Priority)</code></li>
 *   <li>Change the <i>next request priority</i> of the current thread by invoking <code>((ServiceProxy)SERVICE).getServiceContext().setNextPriority(Priority)</code></li>
 * </ol>
 *
 * @param <T> the type of the service interface
 */
@SuppressWarnings("WeakerAccess")
public class ServiceMessageClient<T extends Service> implements MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(ServiceMessageClient.class);
  private static final String OBJECT_EQUALS = "equals";
  private static final String OBJECT_HASH_CODE = "hashCode";
  private static final String OBJECT_TO_STRING = "toString";
  private static final String SERVICE_PROXY_GET_SERVICE_CONTEXT = "getServiceContext";

  //properties
  private final long maxWait;
  private final Class<T> proxyInterface;
  private final RequestSink requestSink;
  private final ServiceContext.Priority defaultPriority;
  private final Map<Class<?>, Function<ResultSet<?>, ? extends ResultSet<?>>> extenderFunctions;
  private final ThreadLocal<ServiceContext.Priority> threadPriority = new ThreadLocal<>();
  private final ThreadLocal<ServiceContext.Priority> nextPriority = new ThreadLocal<>();
  private final ThreadLocal<Integer> nextResponseWindowSize = new ThreadLocal<>();
  private static final ThreadLocal<ServiceContext.Priority> globalThreadPriority = new ThreadLocal<>();

  //variables
  private final AtomicReference<T> proxy = new AtomicReference<>();

  //metrics

  private final LongAdder requests = new LongAdder();
  private final LongAdder streamingInterrupted = new LongAdder();
  private final LongAdder totalRequestTime = new LongAdder();
  private final LongAdder serviceTimeOuts = new LongAdder();

  //constructor

  /**
   * Create a new Service Message Client
   *
   * @param proxyInterface    the class of the service interface
   * @param requestSink       the requestsink to use for messaging. Must be attached to a ServiceMessageHandler on the other side.
   * @param maxWait           the maximum milliseconds to wait for replies from the requestsink (will allow keepalives to extend this)
   * @param defaultPriority   the default priority to use when sending request messages
   * @param extenderFunctions functions to extend resultset return types to subclasses, where subclasses are declared
   */
  private ServiceMessageClient(Class<T> proxyInterface,
                               RequestSink requestSink,
                               long maxWait,
                               ServiceContext.Priority defaultPriority,
                               Map<Class<?>, Function<ResultSet<?>, ? extends ResultSet<?>>> extenderFunctions) {
    this.maxWait = maxWait;
    this.proxyInterface = proxyInterface;
    this.requestSink = requestSink;
    this.defaultPriority = defaultPriority;
    this.extenderFunctions = new ConcurrentHashMap<>(extenderFunctions);
  }

  //interface methods

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
            .addData("requests", requests)
            .addData("streamingInterrupted", streamingInterrupted)
            .addData("serviceTimeOuts", serviceTimeOuts)
            .addData("totalRequestTime", totalRequestTime);
  }


  //public methods

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

  public static ServiceContext.Priority getGlobalThreadPriority() {
    return ifNull(globalThreadPriority.get(), standard);
  }

  /**
   * @return an instance of the proxy interface, which will submit all invoked methods onto the service message bus
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
            new MessageInvocationHandler()
    );
  }

  private class MessageServiceContext implements ServiceContext {
    @Override
    public void setThreadPriority(Priority priority) {
      ServiceMessageClient.this.threadPriority.set(priority);
    }

    @Override
    public void setNextRequestPriority(Priority priority) {
      ServiceMessageClient.this.nextPriority.set(priority);
    }

    public void setNextResponseWindowSize(int responseWindowSize) {
      ServiceMessageClient.this.nextResponseWindowSize.set(responseWindowSize);
    }
  }

  private class MessageInvocationHandler implements InvocationHandler {
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
        return new MessageServiceContext();
      }
      // try invoking
      return invoke(method, arguments);
    }

    private Object invoke(Method method, Object[] arguments) throws Throwable {
      requests.increment();
      //noinspection unused
      try (TimerContext timer = TimerContext.timerMillis(totalRequestTime::add)) {
        UUID requestID = UUID.randomUUID();
        ServiceRequestMessage msg = ServiceRequestMessage.builder()
                .setRequestID(requestID.toString())
                .setPriority(Message.Priority.valueOf(determinePriority().name()))
                .setResponseWindowSize(determineResponseWindowSize())
                .setServiceName(proxyInterface.getName())
                .setMethodName(method.getName())
                .setArgumentTypes(fromTypes(method.getParameterTypes()))
                .setArguments(arguments)
                .build();

        if (LOGGER.isDebug()) {
          LOGGER.debug(">> signal [callID=%s service=%s method=%s]", msg.getRequestID(), msg.getServiceName(), msg.getMethodName());
        }
        //prepare a response handler
        RequestHandler handler = RequestHandler.builder()
                .setCallID(msg.getRequestID())
                .setAllowKeepAlive(true)
                //Make responsequeue double size of the response window size, to ensure we never run out of queue.
                //Response window (flow control) is only active for V4 protocol clients.
                //For V3 protocol clients, the responsequeue is now bounded, but without flow control this may cause slow clients to block other clients.
                .setResponseQueueSize(2 * msg.getResponseWindowSize())
                .setMaxWait(maxWait)
                .build();
        //send the signal
        requestSink.signal(msg, handler, maxWait);
        //return a response handler
        return handleResponses(handler, method);
      }
    }

    private String[] fromTypes(Class<?>[] types) {
      if (types == null) throw new IllegalArgumentException("Types was null!");
      String[] clz = new String[types.length];
      for (int i = 0; i < clz.length; i++) {
        clz[i] = types[i].getName();
      }
      return clz;
    }
  }

  private int determineResponseWindowSize() {
    if (nextResponseWindowSize.get() != null) {
      int size = nextResponseWindowSize.get();
      nextResponseWindowSize.remove();
      return size;
    } else {
      return Message.DEFAULT_RESPONSE_WINDOW_SIZE;
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

  private Object handleResponses(RequestHandler handler, Method invokedMethod) throws Throwable {
    if (ResultSet.class.isAssignableFrom(invokedMethod.getReturnType())) {
      //if method returns a ResultSet, the handler will send a streaming response
      try {
        return handleStreamingResponse(handler, invokedMethod);
      } catch (ServiceTimeOutException ex) {
        serviceTimeOuts.increment();
        throw ex;
      } catch (ResultSetStreamInterruptedException ex) {
        streamingInterrupted.increment();
        throw ex;
      }
    } else {
      //else the handler will return a single response
      return handleValueResponse(handler, invokedMethod);
    }
  }

  private Object handleValueResponse(RequestHandler handler, Method invokedMethod) throws Throwable {
    ServiceResponseMessage response;
    try {
      //wait until response is received, or stream is closed
      response = handler.getNextResponse();
    } catch (InvocationTargetException e) {
      //if we receive error notification from handler, getNextResponse will throw the error
      throw e.getTargetException();
    } catch (MessagingInterruptedException e) {
      //abort request when interrupted
      handler.abort();
      throw new InterruptedException(e.getMessage());
    }
    //if we received no response at all by the timeout limit, this is a service timeout
    if (response == null) {
      handler.timeout();
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< timeout");
      }
      serviceTimeOuts.increment();
      LOGGER.error("ServiceTimeoutException while invoking %s", invokedMethod);
      throw new ServiceTimeOutException(
              String.format("ServiceTimeoutException while waiting for responses for %s", invokedMethod),
              proxyInterface.getName()
      );
    }
    if (LOGGER.isDebug()) {
      LOGGER.debug("<< valueResponse [callID=%s]", response.getCallID());
    }
    //else return single response
    return ((ServiceResponseValueMessage) response).getReturnValue();
  }

  private <R extends ResultSet> R handleStreamingResponse(RequestHandler handler, Method invokedMethod) throws Throwable {
    if (invokedMethod.getReturnType().equals(ResultSet.class)) {
      //create streaming resultset context, counting all RequestHandler errors
      //noinspection unchecked
      return (R) new StreamingResultSetContext<R>(handler, invokedMethod, error -> {
        streamingInterrupted.increment();
        handler.abort();
      });
    } else {
      //if the declared method returns a subclass of ResultSet, we may need an extender function to
      //extend the declared return type to the correct subclass
      Function<ResultSet<?>, ? extends ResultSet<?>> extender = extenderFunctions.computeIfAbsent(invokedMethod.getReturnType(), this::resolveExtenderFunction);
      //convert streaming resultset using extender function
      //noinspection unchecked
      return (R) extender.apply(new StreamingResultSetContext<R>(handler, invokedMethod, error -> {
        streamingInterrupted.increment();
        handler.abort();
      }));
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

  public static <V extends Service> Builder<V> builder() {
    return new Builder<>();
  }

  public static <V extends Service> Builder<V> builder(Class<V> proxyInterface) {
    Builder<V> b = new Builder<>();
    return b.setProxyInterface(proxyInterface);
  }

  public static class Builder<V extends Service> {

    //fields
    private long maxWait;
    private Class<V> proxyInterface;
    private RequestSink requestSink;
    private ServiceContext.Priority defaultPriority = standard;
    private final Map<Class<?>, Function<ResultSet<?>, ? extends ResultSet<?>>> extenderFunctions = new HashMap<>();

    public ServiceMessageClient<V> build() {
      if (proxyInterface == null) throw new IllegalArgumentException("proxyInterface not set");
      if (requestSink == null) throw new IllegalArgumentException("requestSink not set");
      if (maxWait < 0) throw new IllegalArgumentException("maxWait must be a non-negative integer");
      return new ServiceMessageClient<>(proxyInterface, requestSink, maxWait, defaultPriority, Collections.unmodifiableMap(extenderFunctions));
    }

    //setters

    public Builder<V> setDefaultPriority(ServiceContext.Priority defaultPriority) {
      this.defaultPriority = ifNull(defaultPriority, standard);
      return this;
    }

    public <R extends ResultSet<?>> Builder<V> withExtenderFunction(Class<R> returnType, Function<ResultSet<?>, R> extenderFunction) {
      this.extenderFunctions.put(returnType, extenderFunction);
      return this;
    }

    public Builder<V> setMaxWait(long maxWait) {
      this.maxWait = maxWait;
      return this;
    }

    public Builder<V> setProxyInterface(Class<V> proxyInterface) {
      this.proxyInterface = proxyInterface;
      return this;
    }

    public Builder<V> setRequestSink(RequestSink requestSink) {
      this.requestSink = requestSink;
      return this;
    }
  }


}
