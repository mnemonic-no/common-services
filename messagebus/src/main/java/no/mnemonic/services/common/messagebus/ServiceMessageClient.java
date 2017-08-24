package no.mnemonic.services.common.messagebus;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.messaging.requestsink.RequestHandler;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceTimeOutException;
import org.objectweb.asm.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * A Service Message Client acts as a factory, providing an interface implementation which
 * proxies requests across a RequestSink infrastructure.
 *
 * This requires a requestSink which is attached to a ServiceMessageHandler on the other end.
 *
 * @param <T> the type of the service interface
 *
 */
@SuppressWarnings("WeakerAccess")
public class ServiceMessageClient<T extends Service> {

  private static final Logger LOGGER = Logging.getLogger(ServiceMessageClient.class);

  //properties
  private final long maxWait;
  private final Class<T> proxyInterface;
  private final RequestSink requestSink;
  private final Map<Class<?>, Function<ResultSet, ? extends ResultSet>> extenderFunctions;

  //variables
  private final AtomicReference<T> proxy = new AtomicReference<>();

  //metrics

  private final LongAdder requests = new LongAdder();
  private final LongAdder totalRequestTime = new LongAdder();

  //constructor

  /**
   * Create a new Service Message Client
   * @param proxyInterface the class of the service interface
   * @param requestSink the requestsink to use for messaging. Must be attached to a ServiceMessageHandler on the other side.
   * @param maxWait the maximum milliseconds to wait for replies from the requestsink (will allow keepalives to extend this)
   * @param extenderFunctions functions to extend resultset return types to subclasses, where subclasses are declared
   */
  private ServiceMessageClient(Class<T> proxyInterface, RequestSink requestSink, long maxWait, Map<Class<?>, Function<ResultSet, ? extends ResultSet>> extenderFunctions) {
    this.maxWait = maxWait;
    this.proxyInterface = proxyInterface;
    this.requestSink = requestSink;
    this.extenderFunctions = extenderFunctions;
  }

  //interface methods

  //public methods

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
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(proxyInterface);
    enhancer.setCallback(new MessageMethodInterceptor());
    //noinspection unchecked
    return (T) enhancer.create();
  }

  private String[] fromTypes(Type[] types) throws ClassNotFoundException {
    if (types == null) throw new IllegalArgumentException("Types was null!");
    String[] clz = new String[types.length];
    for (int i = 0; i < clz.length; i++) {
      Type t = types[i];
      switch (t.getSort()) {
        case Type.OBJECT:
          clz[i] = t.getClassName();
          break;
        case Type.LONG:
          clz[i] = Long.TYPE.getName();
          break;
        case Type.FLOAT:
          clz[i] = Float.TYPE.getName();
          break;
        case Type.DOUBLE:
          clz[i] = Double.TYPE.getName();
          break;
        case Type.BYTE:
          clz[i] = Byte.TYPE.getName();
          break;
        case Type.INT:
          clz[i] = Integer.TYPE.getName();
          break;
        case Type.BOOLEAN:
          clz[i] = Boolean.TYPE.getName();
          break;
        case Type.SHORT:
          clz[i] = Short.TYPE.getName();
          break;
        case Type.CHAR:
          clz[i] = Character.TYPE.getName();
          break;
        case Type.ARRAY:
          clz[i] = t.getDescriptor().replace("/", ".");
          break;
        default:
          throw new IllegalArgumentException("Invalid argument type: " + t.getSort());
      }
    }
    return clz;
  }

  private class MessageMethodInterceptor implements MethodInterceptor {

    public Object intercept(Object object, Method method, Object[] arguments, MethodProxy proxy)
            throws Throwable {

      if (method.getName().equals("hashCode")) {
        return proxyInterface.hashCode();
      }
      if (method.getName().equals("equals")) {
        return arguments[0] == object;
      }
      // try invoking
      return invoke(proxy, arguments, method.getReturnType());
    }

    private Object invoke(MethodProxy proxy, Object[] arguments, Class<?> declaredReturnType) throws Throwable {
      requests.increment();
      //noinspection unused
      try (TimerContext timer = TimerContext.timerMillis(totalRequestTime::add)) {
        UUID requestID = UUID.randomUUID();
        ServiceRequestMessage msg = ServiceRequestMessage.builder()
                .setRequestID(requestID.toString())
                .setServiceName(proxyInterface.getName())
                .setMethodName(proxy.getSignature().getName())
                .setArgumentTypes(fromTypes(proxy.getSignature().getArgumentTypes()))
                .setArguments(arguments)
                .build();

        if (LOGGER.isDebug()) LOGGER.debug("Signalling request");
        RequestHandler handler = RequestHandler.signal(requestSink, msg, true, maxWait);
        return handleResponses(handler, declaredReturnType);
      } catch (Exception e) {
        LOGGER.error(e, "Error invoking remote method");
        throw e;
      }
    }
  }

  private Object handleResponses(RequestHandler handler, Class<?> declaredReturnType) throws Throwable {
    if (ResultSet.class.isAssignableFrom(declaredReturnType)) {
      //noinspection unchecked
      return handleStreamingResponse(handler, (Class<ResultSet>) declaredReturnType);
    } else {
      return handleValueResponse(handler);
    }
  }

  private Object handleValueResponse(RequestHandler handler) throws Throwable {
    ServiceResponseMessage response;
    try {
      //wait until response is received, or stream is closed
      response = handler.getNextResponse();
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
    if (response == null) throw new ServiceTimeOutException();
    if (LOGGER.isDebug()) LOGGER.debug("Got single response");
    return ((ServiceResponseValueMessage) response).getReturnValue();
  }

  private <R extends ResultSet> R handleStreamingResponse(RequestHandler handler, Class<R> declaredReturnType) throws Throwable {
    if (extenderFunctions.containsKey(declaredReturnType)) {
      ResultSet result = new StreamingResultSetContext(handler);
      //noinspection unchecked
      return (R) extenderFunctions.get(declaredReturnType).apply(result);
    } else if (declaredReturnType.equals(ResultSet.class)) {
      //noinspection unchecked
      return (R) new StreamingResultSetContext(handler);
    } else {
      throw new IllegalStateException("Declared returntype of invoked method is a subclass of ResultSet, but no extender function is defined for this type");
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
    private final Map<Class<?>, Function<ResultSet, ? extends ResultSet>> extenderFunctions = new HashMap<>();

    public ServiceMessageClient<V> build() {
      if (proxyInterface == null) throw new IllegalArgumentException("proxyInterface not set");
      if (requestSink == null) throw new IllegalArgumentException("requestSink not set");
      if (maxWait < 0) throw new IllegalArgumentException("maxWait must be a non-negative integer");
      return new ServiceMessageClient<>(proxyInterface, requestSink, maxWait, Collections.unmodifiableMap(extenderFunctions));
    }

    //setters

    public <R extends ResultSet> Builder<V> withExtenderFunction(Class<R> returnType, Function<ResultSet, R> extenderFunction) {
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
