package no.mnemonic.services.common.api.proxy;

import no.mnemonic.commons.utilities.collections.MapUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ServiceProxyRequestContextTest {

  public static final String CLIENT_IP = "10.0.0.1";

  @Test
  void testContext() {
    assertThrows(IllegalStateException.class, ServiceProxyRequestContext::get);
    try (ServiceProxyRequestContext ignored = ServiceProxyRequestContext.initialize(CLIENT_IP, MapUtils.map())) {
      assertEquals(CLIENT_IP, ServiceProxyRequestContext.get().getClientIP());
    }
    assertThrows(IllegalStateException.class, ServiceProxyRequestContext::get);
  }
}