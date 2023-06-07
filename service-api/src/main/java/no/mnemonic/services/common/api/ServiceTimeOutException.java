package no.mnemonic.services.common.api;

public class ServiceTimeOutException extends RuntimeException {

  private static final long serialVersionUID = 3330507181521367099L;

  private final String interfaceClassName;

  public ServiceTimeOutException(String message, String interfaceClassName) {
    super(message);
    this.interfaceClassName = interfaceClassName;
  }

  public String getInterfaceClassName() {
    return interfaceClassName;
  }
}
