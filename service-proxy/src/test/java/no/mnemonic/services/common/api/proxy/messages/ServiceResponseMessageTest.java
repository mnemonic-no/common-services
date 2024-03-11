package no.mnemonic.services.common.api.proxy.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServiceResponseMessageTest {

    private ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void testSerialization() throws JsonProcessingException {
        ServiceResponseMessage msg = ServiceResponseMessage.builder()
                .setResponse("<serialized response>")
                .setRequestID(UUID.randomUUID())
                .setException("<serialized exception>")
                .build();
        String json = mapper.writeValueAsString(msg);
        System.out.println(json);
        ServiceResponseMessage deserialized = mapper.readValue(json, ServiceResponseMessage.class);
        assertEquals(msg.getRequestID(), deserialized.getRequestID());
        assertEquals(msg.getResponse(), deserialized.getResponse());
        assertEquals(msg.getException(), deserialized.getException());
    }
}