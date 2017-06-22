package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.messaging.requestsink.Message;

import java.io.Serializable;

/**
 * Common response format for the service message bus.
 */
public abstract class ServiceResponseMessage implements Message, Serializable, AppendMembers {

  private static final long serialVersionUID = 6804379968017307020L;

  private final long messageTimestamp = System.currentTimeMillis();
  private final String callID;

  ServiceResponseMessage(String callID) {
    this.callID = callID;
  }

  @Override
  public void appendMembers(StringBuilder buf) {
    AppendUtils.appendField(buf, "callID", callID);
    AppendUtils.appendField(buf, "messageTimestamp", messageTimestamp);
  }

  @Override
  public String toString() {
    return AppendUtils.toString(this);
  }

  @Override
  public String getCallID() {
    return callID;
  }

  @Override
  public long getMessageTimestamp() {
    return messageTimestamp;
  }
}
