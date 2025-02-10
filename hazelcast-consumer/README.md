# Hazelcast-Consumer

## Note

**This package provides the same functionality as `no.mnemonic.services.common:hazelcast5-consumer`,
but is still targeting legacy Hazelcast version 3.x**

This package should be regarded as deprecated, and will be removed in the future.
The `hazelcast5-consumer` package should work as a drop-in replacement for this package, 
as long as the rest of the client code is updated to use Hazelcast 4 or 5.

## Description

The Hazelcast Consumer is a common pattern implementation of a 
Kafka consumer using a Hazelcast queue as buffer.
This solves the problem of Kafka consumer threads which sometimes require too much
time to complete the consuming of data, causing Kafka to drop the client and rebalance the cluster.

Instead, the messages are polled from Kafka into a Hazelcast Queue, which is shared and backed up among 
service instances. The consumer pattern uses a transactional fetch from the Hazelcast Queue,
to ensure that all data in the queue will be handled.

A service node restart will not loose data, as all data is backed up in other service nodes,
and the transactional behaviour will guarantee that the data is either successfully consumed
and removed from the queue, or will be reprocessed by another service node.

**NOTE**: If shutting down ALL service nodes at the same time, there exists a potential
for loosing data currently in the Hazelcast Queue. Make sure to transfer all load/incoming data
to other DC before shutting down the entire service cluster.   

## Usage

You need five components in this pattern:

* A document source (out-of-scope)
* A Kafka-To-Hazelcast handler provider
* A Hazelcast-To-Consumer handler provider
* A Consumer implementation
* A module to bind things together

### The Kafka-To-Hazelcast handler provider

* Should implement LifecycleAspect to stop the handler on shutdown
* Should implement MetricAspect to expose the metrics from the handler implementation

``` java
@Singleton
public class MyCollectorKafkaToHazelcastHandlerProvider implements LifecycleAspect, MetricAspect {

  private final DocumentSource<MyMessage> source;
  private final HazelcastInstance hazelcastInstance;
  private final AtomicReference<KafkaToHazelcastHandler> handler = new AtomicReference<>();

  private long hazelcastQueueOfferTimeoutSec = 5;

  @Inject
  public MyCollectorKafkaToHazelcastHandlerProvider(
          DocumentSource<MyMessage> source,
          HazelcastInstance hazelcastInstance
  ) {
    this.source = source;
    this.hazelcastInstance = hazelcastInstance;
  }

  @Override
  public void startComponent() {
    KafkaToHazelcastHandler<EventCollectorMessage> h = new KafkaToHazelcastHandler<>(
            source,
            hazelcastInstance,
            "QueueName"
    )
            .setHazelcastQueueOfferTimeoutSec(hazelcastQueueOfferTimeoutSec);
    h.startComponent();
    handler.set(h);
  }

  @Override
  public void stopComponent() {
    handler.updateAndGet(h->{
      if (h != null) h.stopComponent();
      return null;
    });
  }

  @Override
  public Metrics getMetrics() {
    return ifNotNull(handler.get(), h-> tryResult(h::getMetrics, new MetricsData()));
  }

}
```

### The Hazelcast-To-Consumer handler provider

* Should implement LifecycleAspect to stop the handler on shutdown
* Should implement MetricAspect to expose the metrics from the handler implementation

``` java
public class MyHazelcastToConsumerHandlerProvider implements LifecycleAspect, MetricAspect {

  @Dependency
  private final Provider<TransactionalConsumer<MyMessage>> consumerProvider;
  @Dependency
  private final HazelcastInstance hazelcastInstance;
  private int workerThreads = 1;

  private final AtomicReference<HazelcastTransactionalConsumerHandler<MyMessage>> handler = new AtomicReference<>();

  @Inject
  public EventCollectorHazelcastToDAOHandlerProvider(
          Provider<TransactionalConsumer<MyMessage>> consumerProvider,
          HazelcastInstance hazelcastInstance
  ) {
    this.consumerProvider = consumerProvider;
    this.hazelcastInstance = hazelcastInstance;
  }

  @Override
  public void startComponent() {
    //create a new consumer handler
    HazelcastTransactionalConsumerHandler<MyMessage> h = new HazelcastTransactionalConsumerHandler<>(
            hazelcastInstance,
            "QueueName",
            consumerProvider::get
    ).setWorkerCount(workerThreads);
    h.startComponent();
    handler.set(h);
  }

  @Override
  public void stopComponent() {
    handler.updateAndGet(h -> {
      if (h != null) h.stopComponent();
      return null;
    });
  }

  @Override
  public Metrics getMetrics() {
    return ifNotNull(handler.get(), h -> tryResult(h::getMetrics, new MetricsData()));
  }

}
```

### The Consumer implementation

* Each consumer worker thread will invoke the consumer provider for an instance of the consumer.
* The consumer implementations must therefore either be thread-safe, or the consumer provider
must return separate instance for each worker thread.
* If the consumer implementation needs to release resources on shutdown, that should be handled by 
the consumer provider.

``` java
@Singleton
public class MyMessageConsumerProvider implements Provider<TransactionalConsumer<MyMessage>>, LifecycleAspect {

  @Override
  public TransactionalConsumer<MyMessage> get() {
    //create a new consumer instance
  }

  @Override
  public void startComponent() {
    //startup
  }

  @Override
  public void stopComponent() {
    //shut down all resources/consumers
  }

}
```

### The Module

The module should:
* bind the singleton handler provider for the Kafka-To-Hazelcast, to ensure lifecycle handling.
* bind the singleton handler provider for the Hazelcast-To-Consumer, to ensure lifecycle handling.
* bind a document source as expected by the Kafka-To-Hazelcast handler provider
* bind a TransactionalConsumer implementation as expected by the Hazelcast-To-Consumer provider 

``` java
public class MyEngineModule extends AbstractModule {

  @Override
  protected void configure() {
  
    install(new MyDocumentSourceModule());

    //engine to pull data from kafka to hazelcast
    bind(MyKafkaToHazelcastHandlerProvider.class).in(Singleton.class);

    //engine to pull data from hazelcast to consumer
    bind(MyHazelcastToConsumerHandlerProvider.class).in(Singleton.class);

    //consumer to bulk index events into ES
    //note: the consumer is NOT a singleton, as each thread may be given a separate consumer each
    bind(new TypeLiteral<TransactionalConsumer<MyMessage>>(){}).toProvider(MyMessageConsumerProvider.class);
  }
}
```
