package no.mnemonic.services.common.api.proxy.serializer;

import no.mnemonic.commons.metrics.MetricAspect;

import java.io.IOException;
import java.util.Base64;

public interface Serializer extends MetricAspect {

  /**
   * @return a serializer identifier, to allow receiver to distinguish between sender formats
   */
  String serializerID();

  /**
   * Serialize the object
   * @param msg object to serialize
   * @return object in serialized format
   * @throws IOException if serialization fails
   */
  byte[] serialize(Object msg) throws IOException;

  /**
   * Serialize the object to base64-string
   * @param msg object to serialize
   * @return object in base64-encoded serialized format
   * @throws IOException if serialization fails
   */
  default String serializeB64(Object msg) throws IOException {
    return Base64.getEncoder().encodeToString(serialize(msg));
  }

  /**
   * Deserialize bytes into a object
   * @param msgbytes object bytes
   * @param classLoader classloader which knows any involved types
   * @param <T> expected object type
   * @return the deserialized object
   * @throws IOException if deserialization fails
   */
  <T> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException;

  /**
   * Deserialize bytes from base64-string, using the current thread classloader to deserialize it.
   * @param base64 the base64 string representing the bytes
   * @return the deserialized object
   * @param <T> expected object type
   * @throws IOException if deserialization fails
   */
  default <T> T deserializeB64(String base64) throws IOException {
    return deserialize(Base64.getDecoder().decode(base64), Thread.currentThread().getContextClassLoader());
  }

}
