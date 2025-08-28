package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.proxy.TestException;
import no.mnemonic.services.common.api.proxy.TestService;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import no.mnemonic.services.common.api.proxy.messages.ServiceResponseMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static no.mnemonic.services.common.api.ServiceContext.Priority.bulk;
import static no.mnemonic.services.common.api.ServiceContext.Priority.standard;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ServiceClientMockTest {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final String SERIALIZED_ARG = "serializedString";
  private static final String DESERIALIZED_VALUE = "deserializedValue";
  private static final String SERIALIZED_VALUE = "serializedValue";

  @Mock
  private ServiceV1HttpClient httpClient;
  @Mock
  private Serializer serializer;
  @Mock
  private ServiceResponseContext responseContext;

  private ServiceClient<TestService> client;
  private TestService serviceProxy;

  @BeforeEach
  void setUp() throws IOException {
    lenient().when(serializer.serializeB64(any())).thenReturn(SERIALIZED_ARG);
    lenient().when(serializer.deserializeB64(any())).thenReturn(DESERIALIZED_VALUE);
    lenient().when(httpClient.request(any(), any(), any(), any(), any())).thenReturn(responseContext);
    lenient().when(responseContext.getContent()).thenReturn(new ByteArrayInputStream(createSerializedValue()));
    client = ServiceClient.<TestService>builder()
            .setProxyInterface(TestService.class)
            .setV1HttpClient(httpClient)
            .setSerializer(serializer)
            .build();
    serviceProxy = client.getInstance();
  }

  private byte[] createSerializedValue() throws JsonProcessingException {
    ServiceResponseMessage msg = ServiceResponseMessage.builder()
            .setResponse(SERIALIZED_VALUE)
            .build();
    return MAPPER.writeValueAsBytes(msg);
  }

  @Test
  void invokeMethod() throws TestException {
    assertEquals(DESERIALIZED_VALUE, serviceProxy.getString("arg"));
    verify(httpClient).request(
            eq(TestService.class.getName()),
            eq("getString"),
            eq(ServiceRequestMessage.Type.single),
            eq(standard),
            argThat(r->
                    r.getArguments().size() == 1
                    && r.getArguments().get(0).equals(SERIALIZED_ARG)
            )
    );
  }

  @Test
  void invokeMethodWithThreadPriorityContext() throws TestException {
    assertEquals(standard, ServiceClient.getGlobalThreadPriority());
    try (ServiceContext.ThreadPriorityContext ctx = ServiceClient.createGlobalThreadPriorityContext(bulk)) {
      assertEquals(bulk, ServiceClient.getGlobalThreadPriority());
      assertEquals(DESERIALIZED_VALUE, serviceProxy.getString("arg"));
    }
    assertEquals(standard, ServiceClient.getGlobalThreadPriority());
  }
}
