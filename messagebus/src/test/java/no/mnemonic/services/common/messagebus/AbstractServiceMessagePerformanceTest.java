package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.testtools.AvailablePortFinder;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.jms.JMSBase;
import no.mnemonic.messaging.requestsink.jms.JMSRequestProxy;
import no.mnemonic.messaging.requestsink.jms.JMSRequestSink;
import no.mnemonic.messaging.requestsink.jms.ProtocolVersion;
import no.mnemonic.messaging.requestsink.jms.serializer.DefaultJavaMessageSerializer;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.network.ConditionalNetworkBridgeFilterFactory;
import org.apache.activemq.network.NetworkConnector;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public abstract class AbstractServiceMessagePerformanceTest extends AbstractServiceMessageTest {

  private static final String INITIAL_CONTEXT_FACTORY_NAME = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
  private static final String CONNECTION_URL = "failover:(tcp://localhost:%d,tcp://localhost:%d)?initialReconnectDelay=100&maxReconnectAttempts=10&timeout=1000";
  private static final String CONNECTION_FACTORY = "ConnectionFactory";
  private static int port1 = AvailablePortFinder.getAvailablePort(10000);
  private static int port2 = AvailablePortFinder.getAvailablePort(20000);
  private static BrokerService broker1;
  private static BrokerService broker2;

  private String serviceQueue;

  @BeforeClass
  public static void setupBrokers() throws Exception {
    broker1 = setupBroker(port1, port2);
    broker2 = setupBroker(port2, port1);
  }

  @AfterClass
  public static void cleanupBrokers() throws Exception {
    broker1.stop();
    broker2.stop();
  }

  public static String getConnectionUrl() {
    return String.format(CONNECTION_URL, port1, port2);
  }

  @Before
  public void prepare() {
    serviceQueue = "TestService" + (int) (Math.random() * 100000);
  }

  <T extends JMSBase.BaseBuilder<T>> T addJMSConnection(T builder) {
    return builder
            .setContextFactoryName(INITIAL_CONTEXT_FACTORY_NAME)
            .setContextURL(getConnectionUrl())
            .setConnectionFactoryName(CONNECTION_FACTORY)
            .setConnectionProperty("trustAllPackages", "true");
  }

  JMSRequestSink createJmsRequestSink() {
    //set up request sink pointing at a vm-local topic
    return addJMSConnection(JMSRequestSink.builder())
            .setPriority(9)
            .setProtocolVersion(ProtocolVersion.V2)
            .setDestinationName("dynamicQueues/" + serviceQueue)
            .build();
  }

  JMSRequestProxy createJMSProxy(ServiceMessageHandler listener, int concurrency) {
    return addJMSConnection(JMSRequestProxy.builder())
            .addSerializer(new DefaultJavaMessageSerializer())
            .setPriority(9)
            .setDestinationName("dynamicQueues/" + serviceQueue)
            .setMaxConcurrentCalls(concurrency)
            .setRequestSink(listener)
            .build();
  }

  private int runTestClient(int threadID, int invocations, int maxWait) {
    JMSRequestSink requestSink = createJmsRequestSink();

    ServiceMessageClient client = ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink)
            .setMaxWait(maxWait)
            .build();
    TestService srv = (TestService) client.getInstance();
    ComponentContainer clientContainer = ComponentContainer.create(requestSink, client);
    clientContainer.initialize();
    //add some sleep to let broker advisories propagate throughout the broker cluster
    LambdaUtils.tryTo(()->Thread.sleep(500));

    int successes = 0;
    int errors = 0;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < invocations; i++) {
      if (LambdaUtils.tryTo(() -> srv.getString("str"))) {
        System.out.println(String.format("Thread#%d - attempt %d - success ", threadID, i));
        successes++;
      } else {
        System.out.println(String.format("Thread#%d - attempt %d - error ", threadID, i));
        errors++;
      }
    }
    long time = System.currentTimeMillis() - startTime;
    double avg = ((double) time) / invocations;
    clientContainer.destroy();
    System.out.println(String.format("Finished client thread#%d in %dms (avg=%.2fms success=%d error=%d)", threadID, time, avg, successes, errors));
    return errors;
  }

  void runParallelClients(int countPerThread, int clientThreads, int maxWait) throws Exception {
    ExecutorService clientExecutor = Executors.newFixedThreadPool(clientThreads*2);

    List<Callable<Integer>> clients = new ArrayList<>();
    for (int i = 0; i < clientThreads; i++) {
      int threadID = i;
      clients.add(() -> runTestClient(threadID, countPerThread, maxWait));
    }

    LongAdder time = new LongAdder();
    int totalErrors;

    try (TimerContext ignored = TimerContext.timerMillis(time::add)) {
      List<Future<Integer>> futures = clientExecutor.invokeAll(clients);
      totalErrors = LambdaUtils.tryStream(futures.stream())
              .map(Future::get)
              .collect(Collectors.toList())
              .stream().mapToInt(Integer::intValue).sum();
    }

    clientExecutor.shutdown();
    int count = countPerThread * clientThreads;
    System.out.println(String.format("Executed %d invocations in %d ms (%.2f ms/req errors=%d)", count, time.longValue(), time.doubleValue() / count, totalErrors));
    assertEquals(0, totalErrors);
  }

  void startContainerAndWaitForProxyConnect(ComponentContainer container, JMSRequestProxy requestProxy) {
    CompletableFuture<Void> waitForConnection = new CompletableFuture<>();
    requestProxy.addJMSRequestProxyConnectionListener(p -> waitForConnection.complete(null));
    container.initialize();
    try {
      waitForConnection.get(1000, TimeUnit.MILLISECONDS);
      System.out.println("JMSRequestProxy connected");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void printHandlerStats(ServiceMessageHandler serviceMessageHandler) {
    try {
      System.out.println("Stats for handler: " + System.identityHashCode(serviceMessageHandler));
      System.out.println("  Total time used: " + serviceMessageHandler.getMetrics().getData("executionTime"));
      System.out.println("  Received requests: " + serviceMessageHandler.getMetrics().getData("receivedRequests"));
      System.out.println("  Keep alives sent: " + serviceMessageHandler.getMetrics().getData("keepAlives"));
      System.out.println("  ResultSet batches sent: " + serviceMessageHandler.getMetrics().getData("resultSetBatches"));
      System.out.println("  Errors: " + serviceMessageHandler.getMetrics().getData("errors"));
      System.out.println();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static BrokerService setupBroker(int thisPort, int otherPort) throws Exception {
    BrokerService broker = new BrokerService();
    broker.setPersistent(false);
    broker.setBrokerName("broker" + thisPort);
    broker.setDestinationPolicy(new PolicyMap());
    broker.getDestinationPolicy().setPolicyEntries(ListUtils.list(
            createDeadLetterStrategyEntry(),
            createNetworkFilterEntry()
    ));
    TransportConnector transportConnector = broker.addConnector("tcp://0.0.0.0:" + thisPort);
    transportConnector.setUpdateClusterClientsOnRemove(true);
    transportConnector.setUpdateClusterClients(true);
    transportConnector.setRebalanceClusterClients(true);
    NetworkConnector networkConnector = broker.addNetworkConnector(String.format("static:(tcp://localhost:%d)", otherPort));
    networkConnector.setNetworkTTL(1);
    networkConnector.setDecreaseNetworkConsumerPriority(true);
    networkConnector.setSuppressDuplicateQueueSubscriptions(true);
    broker.start();
    broker.waitUntilStarted();
    return broker;
  }

  private static PolicyEntry createNetworkFilterEntry() {
    PolicyEntry entry = new PolicyEntry();
    entry.setQueue("Service.>");
    ConditionalNetworkBridgeFilterFactory factory = new ConditionalNetworkBridgeFilterFactory();
    factory.setReplayWhenNoConsumers(true);
    entry.setNetworkBridgeFilterFactory(factory);
    return entry;
  }

  private static PolicyEntry createDeadLetterStrategyEntry() {
    PolicyEntry entry = new PolicyEntry();
    entry.setQueue(">");
    SharedDeadLetterStrategy deadLetterStrategy = new SharedDeadLetterStrategy();
    deadLetterStrategy.setProcessExpired(true);
    entry.setDeadLetterStrategy(deadLetterStrategy);
    return entry;
  }

  <T> Answer<T> createAnswer(int delay) {
    return i -> {
      Thread.sleep(delay);
      return i.getArgument(0);
    };
  }
}
