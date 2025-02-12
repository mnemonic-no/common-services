package no.mnemonic.services.common.api.proxy.client;

import java.lang.reflect.Method;
import java.util.Map;

public interface ServiceClientMetaDataHandler {

  /**
   * Notify this handler with metadata returned from the service proxy.
   * Example is to populate some context or state with the data expected for a certain key.
   *
   * <b>Note</b>: The meaning of keys and value of the value format of the metadata map is convention based,
   * and not something that the framework cares about. The server/client implementation should align with
   * which keys are used for what, and what is the expected data format.
   *
   * @param method   the method that was invoked (to identify the context of the metadata)
   * @param metaData the metadata received from the service proxy invocation
   */
  void handle(Method method, Map<String, String> metaData);
}
