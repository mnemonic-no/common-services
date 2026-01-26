package no.mnemonic.services.common.api.proxy.client;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * The SPI client application can pass in an implementation of this interface
 * to resolve HTTP headers that should be appended to the SPI request
 */
public interface ServiceRequestHeaderResolver {
  Map<String, List<String>> resolveHeaders(Method method, Object[] args);
}
