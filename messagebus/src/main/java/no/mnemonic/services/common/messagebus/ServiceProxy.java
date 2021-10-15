package no.mnemonic.services.common.messagebus;

/**
 * This interface is implicitly implemented by all service proxies returned by <code>ServiceMessageClient</code>.
 * <p>
 * To get hold of the {@link ServiceContext}, the client code should do a cast to <code>ServiceProxy</code>
 * (if the client is injected the service interface and does not control the creation of the proxy,
 * the client should do a defensive cast).
 *
 * Example:
 * <code>
 *   ServiceMessageClient&lt;MyService&gt; client = ServiceMessageClient.builder()......build();
 *   MyService service = client.getInstance();
 *
 *   if (service instanceof ServiceProxy) {
 *     ServiceContext context = ((ServiceProxy)service).getServiceContext();
 *     context.setThreadPriority(bulk);
 *   }
 * </code>
 */
public interface ServiceProxy {

  /**
   * @return the {@link ServiceContext} from this proxy object
   */
  ServiceContext getServiceContext();

}
