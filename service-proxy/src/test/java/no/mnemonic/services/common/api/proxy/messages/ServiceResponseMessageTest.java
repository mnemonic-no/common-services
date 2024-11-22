package no.mnemonic.services.common.api.proxy.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import no.mnemonic.commons.utilities.collections.MapUtils;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ServiceResponseMessageTest {

    private ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void testSerialization() throws JsonProcessingException {
        ServiceResponseMessage msg = ServiceResponseMessage.builder()
                .setMetaData(MapUtils.map(
                        MapUtils.pair("key", "value")
                ))
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
        assertEquals(msg.getMetaData(), deserialized.getMetaData());
    }

    @Test
    void testSerializeEmpty() throws JsonProcessingException {
        ServiceResponseMessage msg = ServiceResponseMessage.builder().build();
        String json = mapper.writeValueAsString(msg);
        System.out.println(json);
        JsonNode deserialized = mapper.readTree(json);
        assertTrue(deserialized.get("response").isNull());
        assertTrue(deserialized.get("exception").isNull());
        assertTrue(deserialized.get("requestID").isNull());
        assertNull(deserialized.get("metaData"));
    }
}