package no.mnemonic.services.common.api.proxy.serializer;

import java.io.IOException;

/**
 * Exception class to handle illegal deserialization.
 */
public class IllegalDeserializationException extends IOException {

  public IllegalDeserializationException(String message) {
    super(message);
  }

}

