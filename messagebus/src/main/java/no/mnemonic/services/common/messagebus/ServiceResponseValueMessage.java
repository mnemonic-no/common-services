package no.mnemonic.services.common.messagebus;

/**
 * Response format for single-value responses in the the service message bus format
 */
public class ServiceResponseValueMessage extends ServiceResponseMessage {

  private static final long serialVersionUID = -2001800924620447114L;

  private final Object returnValue;

  private ServiceResponseValueMessage(String requestID, Object returnValue) {
    super(requestID);
    this.returnValue = returnValue;
  }

  public <T> T getReturnValue() {
    //noinspection unchecked
    return (T) returnValue;
  }

  public static ServiceResponseValueMessage create(String requestID, Object returnValue) {
    return new ServiceResponseValueMessage(requestID, returnValue);
  }

}
