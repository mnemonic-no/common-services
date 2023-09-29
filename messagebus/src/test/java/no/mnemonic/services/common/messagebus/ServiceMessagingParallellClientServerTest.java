package no.mnemonic.services.common.messagebus;


import no.mnemonic.commons.container.ComponentContainer;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.messaging.requestsink.jms.JMSRequestProxy;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.LongAdder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ServiceMessagingParallellClientServerTest extends AbstractServiceMessagePerformanceTest {

  private final Collection<ComponentContainer> serverContainers = new ArrayList<>();
  private final Collection<ServiceMessageHandler> handlers = new ArrayList<>();
  @Mock
  private TestService testService;
  @Mock
  private ServiceSessionFactory sessionFactory;

  @BeforeEach
  public void setup() {
    when(sessionFactory.openSession()).thenReturn(() -> {
    });
  }

  @AfterEach
  public void teardown() {
    serverContainers.forEach(ComponentContainer::destroy);
    handlers.forEach(this::printHandlerStats);
  }

  @Test
  public void testRemoteRequestSingleServer() throws Exception {
    int serverInstances = 1;
    int threadsPerServer = 5;
    int invocationsPerClient = 10;
    int clientThreads = 20;
    int delayPerRequest = 100;
    int clientMaxWait = 5000;

    doRunTest(serverInstances, threadsPerServer, invocationsPerClient, clientThreads, delayPerRequest, clientMaxWait);
  }

  @Test
  public void testRemoteRequestParallellServers() throws Exception {
    int serverInstances = 4;
    int threadsPerServer = 5;
    int invocationsPerClient = 10;
    int clientThreads = 20;
    int delayPerRequest = 500; // Increased delay in an attempt to make test more stable.
    int clientMaxWait = 5000;

    doRunTest(serverInstances, threadsPerServer, invocationsPerClient, clientThreads, delayPerRequest, clientMaxWait);
  }

  //helpers

  private void doRunTest(int serverInstances, int threadsPerServer, int invocationsPerClient, int clientThreads, int delayPerRequest, int clientMaxWait) throws Exception {
    when(testService.getString(any())).thenAnswer(createAnswer(delayPerRequest));

    int totalServerThreads = serverInstances * threadsPerServer;
    long targetTime = (long)delayPerRequest * clientThreads * invocationsPerClient / serverInstances / threadsPerServer;

    System.out.printf("Running %d server instances with %d threads (threadsPerServer=%d delayPerRequest=%d)\n", totalServerThreads, serverInstances, threadsPerServer, delayPerRequest);
    System.out.printf("Executing %d clients with %d requests/client (total %d requests)\n", clientThreads, invocationsPerClient, clientThreads * invocationsPerClient);
    System.out.printf("Target time %dms\n", targetTime);

    for (int i = 0; i < serverInstances; i++) {
      setupServer(threadsPerServer);
    }

    LongAdder timer = new LongAdder();
    try (TimerContext ignored = TimerContext.timerMillis(timer::add)) {
      runParallelClients(invocationsPerClient, clientThreads, clientMaxWait);
    }

    System.out.printf("Target time %dms - Time used %dms\n", targetTime, timer.longValue());
  }

  private void setupServer(int serverThreads) {
    ServiceMessageHandler handler = ServiceMessageHandler.builder()
            .setService(testService)
            .setSessionFactory(sessionFactory)
            .setMaxConcurrentRequests(serverThreads)
            .build();

    JMSRequestProxy requestProxy = createJMSProxy(handler, serverThreads);
    ComponentContainer container = ComponentContainer.create(handler, requestProxy);

    handlers.add(handler);
    serverContainers.add(container);

    startContainerAndWaitForProxyConnect(container, requestProxy);
  }


}
