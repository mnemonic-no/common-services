package no.mnemonic.services.common.api.proxy;

import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import no.mnemonic.services.common.api.proxy.client.ServiceClient;
import no.mnemonic.services.common.api.proxy.client.ServiceV1HttpClient;
import no.mnemonic.services.common.api.proxy.server.ServiceInvocationHandler;
import no.mnemonic.services.common.api.proxy.server.ServiceProxy;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;
import no.mnemonic.services.common.api.proxy.serializer.XStreamSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test that priority-threadpool configuration works
 */
@ExtendWith(MockitoExtension.class)
public class ConcurrencyTest {

  private static final String BASEURL = "http://localhost";
  private static final int BULK_PORT = 9001;
  private static final int DEFAULT_PORT = 9002;
  private static final int EXPEDITE_PORT = 9003;
  public static final int BULK_THREADS = 5;
  public static final int STANDARD_THREADS = 10;
  public static final int EXPEDITE_THREADS = 15;
  public static final int CLIENT_THREADS = 20;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private ServiceProxy proxy;
  @Mock
  private ServiceInvocationHandler.DebugListener debugListener;
  @Mock
  private TestService mockedService;
  @Mock
  private ServiceSessionFactory sessionFactory;
  @Mock
  private ServiceSession session;

  private TestService serviceClient;

  private final AtomicInteger currentThreads = new AtomicInteger();
  private final AtomicInteger maxThreads = new AtomicInteger();

  private final Serializer serializer = XStreamSerializer.builder()
      .setAllowedClass(TestException.class)
      .setAllowedClass(TestArgument.class)
      .build();

  @BeforeEach
  void setUp() throws Exception {
    lenient().when(sessionFactory.openSession()).thenReturn(session);
    lenient().when(mockedService.getString(any())).thenAnswer(i -> {
      int current = currentThreads.incrementAndGet();
      maxThreads.accumulateAndGet(current, Math::max);
      Thread.sleep(1000);
      currentThreads.decrementAndGet();
      return "result";
    });

    ServiceInvocationHandler<TestService> invocationHandler = ServiceInvocationHandler.<TestService>builder()
        .addSerializer(serializer)
        .setExecutorService(executorService)
        .setDebugListener(debugListener)
        .setTimeBetweenKeepAlives(200)
        .setProxiedService(mockedService)
        .setSessionFactory(sessionFactory)
        .build();
    proxy = ServiceProxy.builder()
        .addInvocationHandler(TestService.class, invocationHandler)
        .setBulkPort(BULK_PORT)
        .setBulkThreads(BULK_THREADS)
        .setStandardPort(DEFAULT_PORT)
        .setStandardThreads(STANDARD_THREADS)
        .setExpeditePort(EXPEDITE_PORT)
        .setExpediteThreads(EXPEDITE_THREADS)
        .build();
    proxy.startComponent();

    ServiceV1HttpClient httpClient = ServiceV1HttpClient.builder()
        .setMaxConnections(CLIENT_THREADS)
        .setDebug(true)
        .setBaseURI(BASEURL)
        .build();
    serviceClient = ServiceClient.<TestService>builder()
        .setProxyInterface(TestService.class)
        .setV1HttpClient(httpClient)
        .setSerializer(serializer)
        .build()
        .getInstance();
  }

  @AfterEach
  void tearDown() {
    proxy.stopComponent();
    executorService.shutdown();
  }

  @ParameterizedTest
  @CsvSource(value = {
      "bulk,5",
      "standard,10",
      "expedite,15",
  })
  void testMaxConcurrentServerThreadsStandardPriority(ServiceContext.Priority priority, int expectedMaxThreads)
      throws TestException, ExecutionException, InterruptedException {
    Set<Future<String>> futures = set();
    for (int i = 0; i < CLIENT_THREADS; i++) {
      futures.add(executorService.submit(() -> {
            serviceClient.getServiceContext().setNextRequestPriority(priority);
            return serviceClient.getString("arg");
          }
      ));
    }
    for (Future<String> f : futures) {
      assertEquals("result", f.get());
    }
    verify(mockedService, times(CLIENT_THREADS)).getString("arg");
    assertEquals(expectedMaxThreads, maxThreads.get());
  }
}
