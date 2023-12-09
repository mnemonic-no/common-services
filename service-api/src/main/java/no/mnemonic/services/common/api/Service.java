package no.mnemonic.services.common.api;

/**
 * Common interface for all services.
 * For now a marker interface, but may be extended to define operations a service has to implement.
 */
public interface Service {

  /**
   *
   * @return the ServiceContext for this service proxy
   * @throws NotAProxyException if this is not a proxy.
   */
  default ServiceContext getServiceContext() throws NotAProxyException {
    throw new NotAProxyException();
  }
}
