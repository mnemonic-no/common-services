package no.mnemonic.services.common.api.proxy;

import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ServiceSession;
import no.mnemonic.services.common.api.ServiceSessionFactory;
import no.mnemonic.services.common.api.proxy.client.ServiceClient;
import no.mnemonic.services.common.api.proxy.client.ServiceClientMetaDataHandler;
import no.mnemonic.services.common.api.proxy.client.ServiceV1HttpClient;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;
import no.mnemonic.services.common.api.proxy.serializer.XStreamSerializer;
import no.mnemonic.services.common.api.proxy.server.ServiceInvocationHandler;
import no.mnemonic.services.common.api.proxy.server.ServiceProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

/**
 * Test end-to-end client-to-service via a proxy setup
 */
@ExtendWith(MockitoExtension.class)
public class IntegrationTest {

  private static final String BASEURL = "http://localhost:9001";

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
  @Mock
  private ServiceClientMetaDataHandler metaDataHandler;

  private TestService serviceClient;

  private final Serializer serializer = XStreamSerializer.builder()
      .setAllowedClass(TestException.class)
      .setAllowedClass(TestArgument.class)
      .build();

  @BeforeEach
  void setUp() {
    lenient().when(sessionFactory.openSession()).thenReturn(session);

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
        .build();
    proxy.startComponent();

    ServiceV1HttpClient httpClient = ServiceV1HttpClient.builder()
        .setDebugRequests(true)
        .setBaseURI(BASEURL)
        .build();
    serviceClient = ServiceClient.<TestService>builder()
        .setProxyInterface(TestService.class)
        .setV1HttpClient(httpClient)
        .setSerializer(serializer)
        .withMetaDataHandler(metaDataHandler)
        .withExtender(TestService.MyExtendedResultSet.class, new TestService.MyExtendedResultSetExtender())
        .build()
        .getInstance();
  }

  @AfterEach
  void tearDown() {
    proxy.stopComponent();
    executorService.shutdown();
  }

  @Test
  void testInvokeStringMethod() throws TestException {
    when(mockedService.getString(any())).thenReturn("result");
    assertEquals("result", serviceClient.getString("arg"));
    verify(mockedService).getString("arg");
  }

  @Test
  void testInvokeResultSetMethod() throws TestException {
    when(mockedService.getResultSet(any())).thenReturn(ResultSetImpl.<String>builder()
        .setIterator(ListUtils.list(
            "a", "b", "c"
        ).iterator())
        .build());
    assertEquals(ListUtils.list("a", "b", "c"), ListUtils.list(serviceClient.getResultSet("arg").iterator()));
    verify(mockedService).getResultSet("arg");
  }

  @Test
  void testInvokeResultSetMethodWithNullResult() throws TestException {
    when(mockedService.getResultSet(any())).thenReturn(null);
    assertNull(serviceClient.getResultSet("arg"));
    verify(mockedService).getResultSet("arg");
  }

  @Test
  void testHandleServerSideMetadata() throws TestException {
    when(mockedService.getString(any())).thenAnswer(i->{
      ServiceProxyMetaDataContext.put("metakey", "metavalue");
      return "result";
    });
    assertEquals("result", serviceClient.getString("arg"));
    verify(mockedService).getString("arg");
    verify(metaDataHandler).handle(
            argThat(m->m.getName().equals("getString")),
            argThat(md-> Objects.equals(ListUtils.list(md.get("metakey")), ListUtils.list("metavalue")))
    );
  }


  @Test
  void testInvokeAnnotaedResultSetMethod() throws TestException {
    when(mockedService.getMyAnnotatedResultSet(any())).thenReturn(TestService.MyAnnotatedResultSet.<String>builder()
        .setToken("mytoken")
        .setIterator(ListUtils.list(
            "a", "b", "c"
        ).iterator())
        .build());
    TestService.MyAnnotatedResultSet<String> result = serviceClient.getMyAnnotatedResultSet("arg");
    assertEquals("mytoken", result.getToken());
    assertInstanceOf(TestService.MyAnnotatedResultSet.class, result);
    assertEquals(ListUtils.list("a", "b", "c"), ListUtils.list(result.iterator()));
    verify(mockedService).getMyAnnotatedResultSet("arg");
  }

  @Test
  void testInvokeExtendedResultSetMethod() throws TestException {
    when(mockedService.getMyExtendedResultSet(any())).thenAnswer(i->{
      ServiceProxyMetaDataContext.put(TestService.MyExtendedResultSetExtender.METADATA_KEY, "metavalue");
      return TestService.MyExtendedResultSet.<String>builder()
              .setIterator(ListUtils.list(
                      "a", "b", "c"
              ).iterator())
              .build();
    });
    TestService.MyExtendedResultSet<String> result = serviceClient.getMyExtendedResultSet("arg");
    assertEquals("metavalue", result.getMetaKey());
    assertInstanceOf(TestService.MyExtendedResultSet.class, result);
    assertEquals(ListUtils.list("a", "b", "c"), ListUtils.list(result.iterator()));
    verify(mockedService).getMyExtendedResultSet("arg");
  }

  @Test
  void testSendReceiveLargeResultSet() throws TestException {
    when(mockedService.getResultSet(any())).thenAnswer(i -> ResultSetImpl.<String>builder()
        .setIterator(new Iterator<String>() {
          int counter = 0;

          @Override
          public boolean hasNext() {
            return counter < 10000;
          }

          @Override
          public String next() {
            counter++;
            String str = "result" + counter;
            System.out.println(">> " + str);
            return str;
          }
        })
        .build()
    );
    ResultSet<String> result = serviceClient.getResultSet("arg");
    AtomicInteger counter = new AtomicInteger();
    result.forEach(str -> {
      counter.incrementAndGet();
      System.out.println("<< " + str);
    });
    assertEquals(10000, counter.get());
  }

  @Test
  void testInvokeSlowMethodWithKeepalives() throws TestException {
    when(mockedService.getString(any())).thenAnswer(i -> {
      Thread.sleep(500);
      return "result";
    });
    assertEquals("result", serviceClient.getString("arg"));
    verify(mockedService).getString("arg");
    verify(debugListener, atLeast(2)).keepAliveSent(any());
  }

  @Test
  void testInvokeSlowResultSetWithKeepalives() throws TestException {
    when(mockedService.getResultSet(any())).thenAnswer(i->{
      Thread.sleep(500);
      return ResultSetImpl.<String>builder()
          .setIterator(ListUtils.list(
              "a", "b", "c"
          ).iterator())
          .build();
    });
    assertEquals(ListUtils.list("a", "b", "c"), ListUtils.list(serviceClient.getResultSet("arg").iterator()));
    verify(mockedService).getResultSet("arg");
    verify(debugListener, atLeast(2)).keepAliveSent(any());
  }
}
