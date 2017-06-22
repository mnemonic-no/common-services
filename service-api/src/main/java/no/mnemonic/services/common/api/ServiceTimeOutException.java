package no.mnemonic.services.common.api;

public class ServiceTimeOutException extends RuntimeException {

  private static final long serialVersionUID = 3330507181521367099L;

  public ServiceTimeOutException() {
  }

  public ServiceTimeOutException(String message) {
    super(message);
  }

  public ServiceTimeOutException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServiceTimeOutException(Throwable cause) {
    super(cause);
  }
}
