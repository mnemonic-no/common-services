package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.commons.utilities.StringUtils;
import no.mnemonic.messaging.requestsink.Message;

/**
 * Request format for the service message bus
 */
public class ServiceRequestMessage implements Message, AppendMembers {

  private static final long serialVersionUID = 3118421669978949736L;

  private final long messageTimestamp = System.currentTimeMillis();
  private final String requestID;
  private final String serviceName;
  private final String methodName;
  private final Class[] argumentTypes;
  private final Object[] arguments;

  private ServiceRequestMessage(String requestID, String serviceName, String methodName, Class[] argumentTypes, Object[] arguments) {
    this.requestID = requestID;
    this.serviceName = serviceName;
    this.methodName = methodName;
    this.argumentTypes = argumentTypes;
    this.arguments = arguments;
    validate();
  }

  @Override
  public void appendMembers(StringBuilder buf) {
    AppendUtils.appendField(buf, "requestID", requestID);
    AppendUtils.appendField(buf, "serviceName", serviceName);
    AppendUtils.appendField(buf, "methodName", methodName);
  }

  @Override
  public String toString() {
    return AppendUtils.toString(this);
  }

  @Override
  public String getCallID() {
    return requestID;
  }

  @Override
  public long getMessageTimestamp() {
    return messageTimestamp;
  }

  private void validate() {
    if (StringUtils.isBlank(requestID)) throw new IllegalArgumentException("No requestID set");
    if (StringUtils.isBlank(serviceName)) throw new IllegalArgumentException("No serviceName set");
    if (StringUtils.isBlank(methodName)) throw new IllegalArgumentException("No methodName set");
    if (argumentTypes == null) throw new IllegalArgumentException("No argumentTypes set");
    if (arguments == null) throw new IllegalArgumentException("No arguments set");
    if (arguments.length != argumentTypes.length) throw new IllegalArgumentException("Mismatch between argumentTypes and arguments");
  }

  String getRequestID() {
    return requestID;
  }

  String getServiceName() {
    return serviceName;
  }

  String getMethodName() {
    return methodName;
  }

  Class[] getArgumentTypes() {
    return argumentTypes;
  }

  Object[] getArguments() {
    return arguments;
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {

    private String requestID;
    private String serviceName;
    private String methodName;
    private Class[] argumentTypes;
    private Object[] arguments;

    private Builder() {
    }

    ServiceRequestMessage build() {
      return new ServiceRequestMessage(requestID, serviceName, methodName, argumentTypes, arguments);
    }

    Builder setRequestID(String requestID) {
      this.requestID = requestID;
      return this;
    }

    Builder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    Builder setMethodName(String methodName) {
      this.methodName = methodName;
      return this;
    }

    Builder setArgumentTypes(Class[] argumentTypes) {
      this.argumentTypes = argumentTypes;
      return this;
    }

    Builder setArguments(Object[] arguments) {
      this.arguments = arguments;
      return this;
    }

  }
}
