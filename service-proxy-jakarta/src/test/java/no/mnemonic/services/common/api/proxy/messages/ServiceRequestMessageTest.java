package no.mnemonic.services.common.api.proxy.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import no.mnemonic.services.common.api.ServiceContext;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ServiceRequestMessageTest {

    private ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void testSerialization() throws JsonProcessingException {
        ServiceRequestMessage msg = ServiceRequestMessage.builder()
                .setSerializerID("SERIALIZER")
                .setRequestID(UUID.randomUUID())
                .addArgument(String.class, "<serialized-string>")
                .addArgument(Integer.class, "<serialized-integer>")
                .setMessageTimestamp(10000)
                .setPriority(ServiceContext.Priority.standard)
                .build();
        String json = mapper.writeValueAsString(msg);
        System.out.println(json);
        ServiceRequestMessage deserialized = mapper.readValue(json, ServiceRequestMessage.class);
        assertEquals(msg.getRequestID(), deserialized.getRequestID());
        assertEquals(msg.getArguments(), deserialized.getArguments());
        assertEquals(msg.getPriority(), deserialized.getPriority());
        assertEquals(msg.getSerializerID(), deserialized.getSerializerID());
        assertEquals(msg.getMessageTimestamp(), deserialized.getMessageTimestamp());
    }
}