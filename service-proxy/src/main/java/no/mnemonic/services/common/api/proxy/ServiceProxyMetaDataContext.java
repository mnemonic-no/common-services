
package no.mnemonic.services.common.api.proxy;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple threadlocal utility to store metadata during execution, which should be
 * transported back to the client side as part of the ServiceProxy response.
 *
 * The context is automatically set up for every invocation executed by the ServiceInvocationHandler.
 * The service implementation should populate any metadata to return during code execution, e.g.
 * in an aspect.
 *
 * <code>
 *    ServiceProxyMetaDataContext.put("somekey", "somevalue")
 * </code>
 *
 * On the client side, the client
 *
 * <code>
 * ServiceClient.builder()
 *   ...
 *   .withMetaDataHandler(new MyMetaDataHandler())
 *   .build();
 *
 *
 * public class MyMetaDataHandler implements ServiceClientMetaDataHandler {
 *    public void handle(Method method, Map&lt;String, String&gt; metaData) {
 *      //do something with metadata
 *    }
 * }
 * </code>
 *
 * <b>Note</b>: The meaning of keys and value of the value format of the metadata map is convention based,
 * and not something that the framework cares about. The server/client implementation should align with
 * which keys are used for what, and what is the expected data format.
 *
 * @see no.mnemonic.services.common.api.proxy.client.ServiceClientMetaDataHandler
 * @see no.mnemonic.services.common.api.ResultSetExtender
 */
public class ServiceProxyMetaDataContext implements Closeable {

  private static final ThreadLocal<ServiceProxyMetaDataContext> CONTEXT = new ThreadLocal<>();

  private final Map<String, String> metaData = new HashMap<>();

  /**
   * Initialize a new context for the current thread.
   * NOTE: This will overwrite any existing context for the current thread.
   *
   * @return the initialized context
   */
  public static ServiceProxyMetaDataContext initialize() {
    ServiceProxyMetaDataContext ctx = new ServiceProxyMetaDataContext();
    CONTEXT.set(ctx);
    return ctx;
  }

  /**
   * Set an existing context as active context for this thread.
   * This may be useful if delegation execution to another thread, picking up
   * metadata from the delegated thread into the current context.
   *
   * NOTE: any context already set for the current thread will be dropped.
   *
   * @param existingContext context an existing context to set as active context for the current thread.
   */
  public static void set(ServiceProxyMetaDataContext existingContext) {
    if (existingContext == null) throw new IllegalArgumentException("Context not set");
    CONTEXT.set(existingContext);
  }

  /**
   * @return the metadata set in this context
   * @throws IllegalStateException if no context is set
   */
  public static Map<String, String> getMetaData() {
    ServiceProxyMetaDataContext ctx = CONTEXT.get();
    if (ctx == null) throw new IllegalStateException("No context set");
    return ctx.metaData;
  }

  /**
   *
   * @return true if the context is set for this thread
   */
  public static boolean isSet() {
    return CONTEXT.get() != null;
  }

  @Override
  public void close() {
    CONTEXT.remove();
  }

  /**
   * Write a metadata entry into the current context.
   *
   * @param key   the metadata key. If this key already exists in the current context, the value is overwritten.
   * @param value a metadata value
   * @throws IllegalStateException if no context is set
   */
  public static void put(String key, String value) {
    if (key == null || value == null) return;
    ServiceProxyMetaDataContext ctx = CONTEXT.get();
    if (ctx == null) throw new IllegalStateException("No context set");
    ctx.metaData.put(key, value);
  }

  /**
   * Append a metadata map to the current context.
   * Any keys which already exist will have its values overwritten.
   *
   * @param metaData The data to append to the context
   * @throws IllegalStateException if no context is set
   */
  public static void putAll(Map<String, String> metaData) {
    if (metaData == null) return;
    ServiceProxyMetaDataContext ctx = CONTEXT.get();
    if (ctx == null) throw new IllegalStateException("No context set");
    ctx.metaData.putAll(metaData);
  }

}
