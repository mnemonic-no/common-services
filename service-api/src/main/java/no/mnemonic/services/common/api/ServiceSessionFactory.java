package no.mnemonic.services.common.api;

/**
 * A service session factory provides sessions which envelope a service call.
 * See {@link Service}
 */
public interface ServiceSessionFactory extends AutoCloseable {

  /**
   * Service sessions which are limited to the execution of the service method are typically
   * closed when the service method returns. To allow the session to stay open for lazy iteration of
   * a resultset, open a session before calling the service method, and close it after
   * handling of the resultset is finished.
   *
   * @return a session surrounding subsequent service calls.
   * @see ServiceSession
   */
  ServiceSession openSession();

}
