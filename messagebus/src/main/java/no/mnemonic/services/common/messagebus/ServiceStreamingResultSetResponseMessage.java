package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.utilities.AppendUtils;

import java.util.Collection;

/**
 * Response format for multi-value responses in the the service message bus format.
 * This format allows streaming resultsets.
 */
@SuppressWarnings("WeakerAccess")
public class ServiceStreamingResultSetResponseMessage extends ServiceResponseMessage {

  private static final long serialVersionUID = 2244171240129728220L;

  private final int limit;
  private final int offset;
  private final int index;
  private final int count;
  private final Collection<?> batch;
  private final boolean lastMessage;

  private ServiceStreamingResultSetResponseMessage(String requestID, int limit, int offset, int count, int index, Collection<?> batch, boolean lastMessage) {
    super(requestID);
    this.limit = limit;
    this.offset = offset;
    this.index = index;
    this.count = count;
    this.batch = batch;
    this.lastMessage = lastMessage;
  }

  @Override
  public void appendMembers(StringBuilder buf) {
    super.appendMembers(buf);
    AppendUtils.appendField(buf, "index", index);
    AppendUtils.appendField(buf, "count", count);
    AppendUtils.appendField(buf, "limit", limit);
    AppendUtils.appendField(buf, "offset", offset);
    AppendUtils.appendField(buf, "lastMessage", lastMessage);
  }

  public Collection<?> getBatch() {
    return batch;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public int getCount() {
    return count;
  }

  public int getIndex() {
    return index;
  }

  public boolean isLastMessage() {
    return lastMessage;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    //fields
    private String requestID;
    private int limit;
    private int offset;
    private int count;
    private boolean lastMessage;

    public ServiceStreamingResultSetResponseMessage build(int index, Collection<?> batch) {
      return new ServiceStreamingResultSetResponseMessage(requestID, limit, offset, count, index, batch, lastMessage);
    }

    public ServiceStreamingResultSetResponseMessage build(int index, Collection<?> batch, boolean lastMessage) {
      return new ServiceStreamingResultSetResponseMessage(requestID, limit, offset, count, index, batch, lastMessage);
    }

    //setters

    public Builder setLastMessage(boolean lastMessage) {
      this.lastMessage = lastMessage;
      return this;
    }

    public Builder setRequestID(String requestID) {
      this.requestID = requestID;
      return this;
    }

    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder setOffset(int offset) {
      this.offset = offset;
      return this;
    }

    public Builder setCount(int count) {
      this.count = count;
      return this;
    }

  }

}
