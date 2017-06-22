package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.commons.testtools.AvailablePortFinder;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.requestsink.jms.JMSConnection;
import no.mnemonic.messaging.requestsink.jms.JMSConnectionImpl;
import no.mnemonic.messaging.requestsink.jms.JMSRequestProxy;
import no.mnemonic.messaging.requestsink.jms.JMSRequestSink;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public abstract class AbstractServiceMessagePerformanceTest extends AbstractServiceMessageTest {

  private int port1 = AvailablePortFinder.getAvailablePort(10000);
  private int port2 = AvailablePortFinder.getAvailablePort(20000);
  private BrokerService broker1;
  private BrokerService broker2;

  @Before
  public final void setupBrokers() throws Exception {
    broker1 = setupBroker(port1, port2);
    broker2 = setupBroker(port2, port1);
  }

  @After
  public final void cleanupBrokers() throws Exception {
    broker1.stop();
    broker2.stop();
  }


  JMSConnection createJMSConnection() {
    return JMSConnectionImpl.builder()
            .setContextFactoryName("org.apache.activemq.jndi.ActiveMQInitialContextFactory")
            .setContextURL(String.format("failover:(tcp://localhost%d,tcp://localhost:%d)?initialReconnectDelay=100", port1, port2))
            .setConnectionFactoryName("ConnectionFactory")
            .setProperty("trustAllPackages", "true")
            .build();
  }

  JMSRequestSink createJmsRequestSink(JMSConnection connection) {
    //set up request sink pointing at a vm-local topic
    return JMSRequestSink.builder()
            .addConnection(connection)
            .setDestinationName("dynamicQueues/TestService")
            .build();
  }

  JMSRequestProxy createJMSProxy(JMSConnection connection, ServiceMessageHandler listener, int concurrency) {
    return JMSRequestProxy.builder()
            .addConnection(connection)
            .setDestinationName("dynamicQueues/TestService")
            .setMaxConcurrentCalls(concurrency)
            .setRequestSink(listener)
            .build();
  }

  private int runTestClient(int threadID, int invocations, int maxWait) {
    JMSConnection connection = createJMSConnection();
    JMSRequestSink requestSink = createJmsRequestSink(connection);

    ServiceMessageClient client = ServiceMessageClient.builder(TestService.class)
            .setRequestSink(requestSink)
            .setMaxWait(maxWait)
            .build();
    TestService srv = (TestService) client.getInstance();
    ComponentContainer clientContainer = ComponentContainer.create(connection, requestSink, client);

    int successes = 0;
    int errors = 0;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < invocations; i++) {
      if (LambdaUtils.tryTo(() -> srv.getString("str"))) {
        successes++;
      } else {
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
    ExecutorService clientExecutor = Executors.newFixedThreadPool(clientThreads);

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

  private BrokerService setupBroker(int thisPort, int otherPort) throws Exception {
    BrokerService broker = new BrokerService();
    broker.setPersistent(false);
    broker.setBrokerName("broker" + thisPort);
    broker.addConnector("tcp://0.0.0.0:" + thisPort);
    broker.addNetworkConnector(String.format("static:(tcp://localhost:%d)", otherPort));
    broker.start();
    broker.waitUntilStarted();
    return broker;
  }

  <T> Answer<T> createAnswer(int delay) {
    return i -> {
      Thread.sleep(delay);
      return i.getArgument(0);
    };
  }
}
