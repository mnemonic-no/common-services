package no.mnemonic.services.common.hazelcast.consumer;

import com.hazelcast.core.HazelcastInstance;
import no.mnemonic.commons.component.Dependency;
import no.mnemonic.commons.component.LifecycleAspect;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.MetricsData;

import javax.inject.Provider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;

/**
 * @deprecated Use <code>hazelcast5-consumer</code> package instead
 */
@Deprecated
public class HazelcastTransactionalConsumerHandler<T> extends HazelcastTransactionalConsumer<T> implements LifecycleAspect, MetricAspect {

  private static final Logger LOG = Logging.getLogger(HazelcastTransactionalConsumerHandler.class);

  private static final int DEFAULT_WORKERS_COUNT = 1;
  private static final String CORRELATION_ID = "correlationID";
  private static final Duration DEFAULT_TERMINATION_TIMEOUT = Duration.ofSeconds(10);

  @Dependency
  private final Provider<TransactionalConsumer<T>> consumerProvider;

  private int workerCount = DEFAULT_WORKERS_COUNT;
  private Duration terminationTimeout = DEFAULT_TERMINATION_TIMEOUT;

  private final AtomicInteger runningWorkers = new AtomicInteger();
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<ExecutorService> workerPool = new AtomicReference<>();
  private final Collection<WorkerLifecycleListener> workerLifecycleListeners = new ArrayList<>();

  public HazelcastTransactionalConsumerHandler(
          HazelcastInstance hazelcastInstance,
          String hazelcastQueueName,
          Provider<TransactionalConsumer<T>> consumerProvider) {
    super(hazelcastInstance, hazelcastQueueName);
    if (consumerProvider == null) throw new IllegalArgumentException("consumerProvider not provided");
    this.consumerProvider = consumerProvider;
  }

  @Override
  public void startComponent() {
    LOG.info("Start consumer for queue %s with %d concurrent sync workers", getHazelcastQueueName(), workerCount);

    if (getBulkSize() * getHazelcastQueuePollTimeoutSec() >= getHazelcastTransactionTimeoutSec()) {
      // Prevent HZ transaction time out when polling from HZ for bulkSize amount of data,
      // so that the time waiting for poll would not exceed the HZ transaction time out.
      throw new IllegalStateException(String.format("bulkSize (%d) * HZ poll timeout (%ds) must be less than HZ transaction timeout (%ds).",
              getBulkSize(), getHazelcastQueuePollTimeoutSec(), getHazelcastTransactionTimeoutSec()));
    }

    ensureWorkerPool();
    for (int i = 0; i < workerCount; i++) {
      workerPool.get().submit(this::runWorker);
    }
    running.set(true);
  }

  @Override
  public void stopComponent() {
    LOG.info("Stop %s", getClass().getSimpleName());
    running.set(false);
    workerPool.updateAndGet(p -> {
      try {
        p.shutdown();
        p.awaitTermination(terminationTimeout.getSeconds(), TimeUnit.SECONDS);
        return null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted when shutdown thread pool", e);
      }
    });
  }

  @Override
  public MetricsData getMetrics() throws MetricException {
    return super.getMetrics()
            .addData("alive", running.get() && runningWorkers.get() > 0 ? 1 : 0)
            .addData("workers.all.alive", running.get() && runningWorkers.get() == workerCount ? 1 : 0)
            .addData("workers.running.count", runningWorkers.get());
  }

  @Override
  protected boolean isShutdown() {
    return ifNotNull(workerPool.get(), ExecutorService::isShutdown, true);
  }

  private void runWorker() {
    LOG.info("%s start", Thread.currentThread().getName());
    runningWorkers.incrementAndGet();

    try (TransactionalConsumer<T> consumer = consumerProvider.get()) {
      while (!ifNotNull(workerPool.get(), ExecutorService::isShutdown, true)) {
        String originalCorrelationID = Logging.getLoggingContext().get(CORRELATION_ID);
        Logging.getLoggingContext().put(CORRELATION_ID, UUID.randomUUID().toString());
        try {
          consumeNextBatch(consumer);
        } finally {
          Logging.getLoggingContext().put(CORRELATION_ID, originalCorrelationID);
        }
      }
    } catch (Exception e) {
      // Catch all exceptions that fail the thread
      LOG.error(e, "%s failed and stop that caused by: %s", Thread.currentThread().getName(), e.getMessage());
    } finally {
      LOG.info("%s finished", Thread.currentThread().getName());
      runningWorkers.decrementAndGet();
      workerLifecycleListeners.forEach(l -> l.workerStopped(Thread.currentThread()));
    }
  }

  private void ensureWorkerPool() {
    workerPool.updateAndGet(p -> {
      if (p != null) return p;

      AtomicLong threadsCount = new AtomicLong(0);
      return Executors.newFixedThreadPool(workerCount, runnable -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName(String.format("%s-Consumer-%d", getHazelcastQueueName(), threadsCount.getAndIncrement()));
        thread.setUncaughtExceptionHandler((t, e) -> LOG.error(e, "Uncaught exception from thread %s: %s", t.getName(), e.getMessage()));
        return thread;
      });
    });
  }

  public HazelcastTransactionalConsumerHandler<T> setWorkerCount(int workerCount) {
    this.workerCount = workerCount;
    return this;
  }

  public HazelcastTransactionalConsumerHandler<T> setTerminationTimeout(Duration terminationTimeout) {
    this.terminationTimeout = terminationTimeout;
    return this;
  }

  //only for testing
  HazelcastTransactionalConsumerHandler<T> setWorkerPool(ExecutorService workerPool) {
    this.workerPool.set(workerPool);
    return this;
  }

  HazelcastTransactionalConsumerHandler<T> addWorkerLifecycleListener(WorkerLifecycleListener l) {
    this.workerLifecycleListeners.add(l);
    return this;
  }

  interface WorkerLifecycleListener {
    void workerStopped(Thread thread);
  }
}
