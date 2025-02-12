package no.mnemonic.services.common.api.proxy;

import no.mnemonic.commons.utilities.collections.MapUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ServiceProxyMetaDataContextTest {

  @Test
  public void deleteContextOnClose() {
    try (ServiceProxyMetaDataContext ctx = ServiceProxyMetaDataContext.initialize()) {
      assertNotNull(ServiceProxyMetaDataContext.getMetaData());
    }
    assertThrows(IllegalStateException.class, ServiceProxyMetaDataContext::getMetaData);
  }

  @Test
  public void appendToContext() {
    Map<String, String> metaData;
    try (ServiceProxyMetaDataContext ctx = ServiceProxyMetaDataContext.initialize()) {

      ServiceProxyMetaDataContext.put("key1", "value1");
      ServiceProxyMetaDataContext.put("key2", "value2");
      ServiceProxyMetaDataContext.put("key1", "value3");
      ServiceProxyMetaDataContext.put("key2", "value4");

      metaData = ServiceProxyMetaDataContext.getMetaData();
    }

    assertEquals("value3", metaData.get("key1"));
    assertEquals("value4", metaData.get("key2"));
  }

  @Test
  public void putAllToContext() {
    Map<String, String> metaData = MapUtils.map(
            MapUtils.pair("key1", "value1")
    );
    try (ServiceProxyMetaDataContext ctx = ServiceProxyMetaDataContext.initialize()) {
      ServiceProxyMetaDataContext.putAll(metaData);
      assertEquals("value1", ServiceProxyMetaDataContext.getMetaData().get("key1"));
    }

  }

  @Test
  public void failPutWithoutContext() {
    assertThrows(IllegalStateException.class, ()->ServiceProxyMetaDataContext.put("key1", "value1"));
  }
}