package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.Message;
import no.mnemonic.messaging.requestsink.RequestContext;
import no.mnemonic.messaging.requestsink.RequestSink;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import no.mnemonic.services.common.api.annotations.ResultBatchSize;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class ServiceMessageHandler implements RequestSink, LifecycleAspect, MetricAspect {

  private static Clock clock = Clock.systemUTC();
  private static final Logger LOGGER = Logging.getLogger(ServiceMessageHandler.class);
  private static final long DEFAULT_SHUTDOWN_WAIT_MS = 10000;
  private static final long DEFAULT_KEEPALIVE_INTERVAL = 1000;
  private static final int DEFAULT_KEEPALIVE_MULTIPLIER = 5;
  private static final int DEFAULT_BATCH_SIZE = 100;
  private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 10;

  //variables
  @Dependency
  private final Service service;
  private final ServiceSessionFactory sessionFactory;
  private final int maxConcurrentRequests;
  private final int batchSize;
  private final long keepAliveInterval;
  private final int keepAliveMultiplier;
  private final long shutdownWait;
  private ExecutorService executor;

  //metrics
  private final LongAdder ignoredRequests = new LongAdder();
  private final LongAdder receivedRequests = new LongAdder();
  private final LongAdder keepAlives = new LongAdder();
  private final LongAdder resultSetBatches = new LongAdder();
  private final LongAdder executionTime = new LongAdder();
  private final LongAdder handlingTime = new LongAdder();
  private final LongAdder exceptions = new LongAdder();
  private final LongAdder undeclaredExceptions = new LongAdder();
  private final LongAdder runningRequests = new LongAdder();
  private final LongAdder pendingRequests = new LongAdder();

  private ServiceMessageHandler(Service service, ServiceSessionFactory sessionFactory, int maxConcurrentRequests, int batchSize, long keepAliveInterval, int keepAliveMultiplier, long shutdownWait) {
    if (service == null) throw new IllegalArgumentException("service not set");
    if (sessionFactory == null) throw new IllegalArgumentException("sessionFactory not set");
    if (maxConcurrentRequests < 1)
      throw new IllegalArgumentException("maxConcurrentRequests must be a positive integer");
    if (batchSize < 1) throw new IllegalArgumentException("batchSize must be a positive integer");
    if (keepAliveMultiplier < 1) throw new IllegalArgumentException("keepAliveMultiplier must be a positive integer");
    if (keepAliveInterval < 1) throw new IllegalArgumentException("keepAliveInterval must be a positive integer");
    if (shutdownWait < 1) throw new IllegalArgumentException("shutdownWait must be a positive integer");
    this.keepAliveMultiplier = keepAliveMultiplier;
    this.sessionFactory = sessionFactory;
    this.service = service;
    this.maxConcurrentRequests = maxConcurrentRequests;
    this.batchSize = batchSize;
    this.keepAliveInterval = keepAliveInterval;
    this.shutdownWait = shutdownWait;
  }

  @Override
  public void startComponent() {
    executor = Executors.newFixedThreadPool(
            maxConcurrentRequests,
            new ThreadFactoryBuilder().setNamePrefix("ServiceMessageHandler").build()
    );
  }

  @Override
  public void stopComponent() {
    LOGGER.info("Shutting down handler");
    if (executor != null) {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(shutdownWait, TimeUnit.MILLISECONDS)) {
          LOGGER.warning("Executor still not finished");
        }
      } catch (InterruptedException e) {
        LOGGER.warning(e, "Error waiting for executor shutdown");
        Thread.currentThread().interrupt();
      }
    }
  }

  @SuppressWarnings("WeakerAccess")
  public MetricsData getMetrics() throws MetricException {
    return new MetricsData()
            .addData("ignoredRequests", ignoredRequests)
            .addData("receivedRequests", receivedRequests)
            .addData("keepAlives", keepAlives)
            .addData("resultSetBatches", resultSetBatches)
            .addData("executionTime", executionTime)
            .addData("handlingTime", handlingTime)
            .addData("exceptions", exceptions)
            .addData("undeclaredExceptions", undeclaredExceptions)
            .addData("runningRequests", runningRequests)
            .addData("pendingRequests", pendingRequests);
  }

  @Override
  public <T extends RequestContext> T signal(Message msg, T signalContext, long maxWait) {
    if (executor == null) {
      throw new IllegalStateException("Received signal before executor is set, is component started?");
    }
    if (msg == null) throw new IllegalStateException("Message not provided");
    if (signalContext == null) throw new IllegalStateException("Signal context not provided");

    if (!(msg instanceof ServiceRequestMessage)) {
      LOGGER.warning("Received unexpected signal: " + msg.getClass());
      ignoredRequests.increment();
      return signalContext;
    }

    receivedRequests.increment();
    pendingRequests.increment();

    try (TimerContext ignored = TimerContext.timerMillis(handlingTime::add)) {
      ServiceRequestMessage request = (ServiceRequestMessage) msg;
      if (LOGGER.isDebug()) {
        LOGGER.debug("<< signal [callID=%s service=%s method=%s]", request.getRequestID(), request.getServiceName(), request.getMethodName());
      }
      //execute actual method invocation in separate thread
      Future<?> future = executor.submit(() -> handleRequest(request, signalContext));

      //initial keepalive
      sendKeepAlive(signalContext, request.getRequestID());
      while (!LambdaUtils.tryTo(() -> future.get(keepAliveInterval, TimeUnit.MILLISECONDS)) && !future.isDone()) {
        //send keepalive while request is being handled
        if (sendKeepAlive(signalContext, request.getRequestID())) {
          keepAlives.increment();
        } else {
          LOGGER.warning("Interrupting callID=%s due to failed keepalive", request.getRequestID());
          future.cancel(true);
        }
      }
      //when future is resolved, the handler is done, so we can return
    }
    return signalContext;
  }

  //private methods

  private boolean sendKeepAlive(RequestContext signalContext, String callID) {
    //send keepalive with timeout twice the keepalive interval (so channel is not closed before next keepalive arrives)
    long until = clock.millis() + (keepAliveMultiplier * keepAliveInterval);
    if (LOGGER.isDebug()) {
      LOGGER.debug(">> keepalive [callID=%s until=%s]", callID, new Date(until));
    }
    return signalContext.keepAlive(until);
  }

  private void handleRequest(ServiceRequestMessage request, RequestContext signalContext) {
    pendingRequests.decrement();
    runningRequests.increment();

    try (ServiceSession ignored = sessionFactory.openSession()) {

      //determine method to invoke
      Method method = service.getClass().getMethod(request.getMethodName(), parseTypes(request.getArgumentTypes()));

      Object returnValue;
      //noinspection unused
      try (TimerContext timer = TimerContext.timerMillis(executionTime::add)) {
        if (LOGGER.isDebug()) {
          LOGGER.debug("# invoke [method=%s]", method);
        }
        returnValue = method.invoke(service, request.getArguments());
      }

      if (returnValue instanceof ResultSet) {
        //if result is a ResultSet, perform streaming result set handling
        handleResultSet(request.getRequestID(), method, (ResultSet) returnValue, signalContext);
      } else {
        //else send single object response
        if (LOGGER.isDebug()) {
          LOGGER.debug(">> addResponse [callID=%s]", request.getRequestID());
        }
        signalContext.addResponse(ServiceResponseValueMessage.create(request.getRequestID(), returnValue));
      }

    } catch (InvocationTargetException e) {
      handleException(signalContext, e.getTargetException());
    } catch (Exception e) {
      handleException(signalContext, e);
    } finally {
      runningRequests.decrement();
      //finally, on either normal result or exception, signal end-of-stream
      if (LOGGER.isDebug()) {
        LOGGER.debug(">> endOfStream [callID=%s]", request.getRequestID());
      }
      signalContext.endOfStream();
    }
  }

  private void handleException(RequestContext signalContext, Throwable e) {
    exceptions.increment();
    if (isUndeclaredException(e)) {
      //undeclared exceptions are concidered non-expected, and are counted and logged
      undeclaredExceptions.increment();
      LOGGER.warning(e, "Error handling request");
    }
    //send error signal to client
    signalContext.notifyError(e);
  }

  private Class[] parseTypes(String[] types) throws ClassNotFoundException {
    if (types == null) throw new IllegalArgumentException("Types was null!");
    Class[] clz = new Class[types.length];
    for (int i = 0; i < clz.length; i++) {
      String typename = types[i];
      if (Objects.equals(Long.TYPE.getName(), typename)) {
        clz[i] = Long.TYPE;
      } else if (Objects.equals(Integer.TYPE.getName(), typename)) {
        clz[i] = Integer.TYPE;
      } else if (Objects.equals(Float.TYPE.getName(), typename)) {
        clz[i] = Float.TYPE;
      } else if (Objects.equals(Double.TYPE.getName(), typename)) {
        clz[i] = Double.TYPE;
      } else if (Objects.equals(Byte.TYPE.getName(), typename)) {
        clz[i] = Byte.TYPE;
      } else if (Objects.equals(Boolean.TYPE.getName(), typename)) {
        clz[i] = Boolean.TYPE;
      } else if (Objects.equals(Short.TYPE.getName(), typename)) {
        clz[i] = Short.TYPE;
      } else if (Objects.equals(Character.TYPE.getName(), typename)) {
        clz[i] = Character.TYPE;
      } else if (Objects.equals("array", typename)) {
        clz[i] = Object[].class;
      } else {
        clz[i] = Class.forName(typename);
      }
    }
    return clz;
  }

  private boolean isUndeclaredException(Throwable e) {
    return e instanceof RuntimeException || e instanceof Error;
  }

  private void handleResultSet(String requestID, Method method, ResultSet resultSet, RequestContext signalContext) {
    ServiceStreamingResultSetResponseMessage.Builder builder = ServiceStreamingResultSetResponseMessage.builder()
            .setRequestID(requestID)
            .setCount(resultSet.getCount())
            .setLimit(resultSet.getLimit())
            .setOffset(resultSet.getOffset());

    int methodBatchSize = determineBatchSize(method);
    int batchIndex = 0;
    Collection<Object> batch = new ArrayList<>();

    for (Object o : resultSet) {
      if (batch.size() >= methodBatchSize) {
        resultSetBatches.increment();
        if (LOGGER.isDebug()) {
          LOGGER.debug(">> addResponseBatch [callID=%s idx=%d size=%d]", requestID, batchIndex, batch.size());
        }
        signalContext.addResponse(builder.build(batchIndex++, batch));
        batch = new ArrayList<>();
      }
      batch.add(o);
    }
    //final batch
    //noinspection UnusedAssignment
    if (LOGGER.isDebug()) {
      LOGGER.debug(">> addResponseBatch [callID=%s idx=%d size=%d last=true]", requestID, batchIndex, batch.size());
    }
    signalContext.addResponse(builder.build(batchIndex, batch, true));
  }

  private int determineBatchSize(Method method) {
    ResultBatchSize batchSize = method.getAnnotation(ResultBatchSize.class);
    if (batchSize == null) return this.batchSize;
    return batchSize.value();
  }

  @SuppressWarnings("WeakerAccess")
  public static Builder builder() {
    return new Builder();
  }

  @SuppressWarnings("WeakerAccess")
  public static class Builder {
    private Service service;
    private ServiceSessionFactory sessionFactory;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS;
    private long keepAliveInterval = DEFAULT_KEEPALIVE_INTERVAL;
    private int keepAliveMultiplier = DEFAULT_KEEPALIVE_MULTIPLIER;
    private long shutdownWait = DEFAULT_SHUTDOWN_WAIT_MS;

    public ServiceMessageHandler build() {
      if (service == null) throw new IllegalArgumentException("service not set");
      return new ServiceMessageHandler(service, sessionFactory, maxConcurrentRequests, batchSize, keepAliveInterval, keepAliveMultiplier, shutdownWait);
    }

    public Builder setShutdownWait(long shutdownWait) {
      this.shutdownWait = shutdownWait;
      return this;
    }

    public Builder setSessionFactory(ServiceSessionFactory sessionFactory) {
      this.sessionFactory = sessionFactory;
      return this;
    }

    public Builder setService(Service service) {
      this.service = service;
      return this;
    }

    public Builder setMaxConcurrentRequests(int maxConcurrentRequests) {
      this.maxConcurrentRequests = maxConcurrentRequests;
      return this;
    }

    public Builder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder setKeepAliveInterval(@SuppressWarnings("SameParameterValue") long keepAliveInterval) {
      this.keepAliveInterval = keepAliveInterval;
      return this;
    }

    public Builder setKeepAliveMultiplier(int keepAliveMultiplier) {
      this.keepAliveMultiplier = keepAliveMultiplier;
      return this;
    }
  }

}
