package no.mnemonic.services.common.api.proxy.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import no.mnemonic.services.common.api.ServiceContext;

import java.util.List;
import java.util.UUID;

/**
 * Request format for the service message bus
 */
@Builder(setterPrefix = "set")
@Getter
@ToString(onlyExplicitlyIncluded = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceRequestMessage {

  private final long messageTimestamp;
  private final UUID requestID;
  private final String serializerID;
  @Singular
  private final List<String> argumentTypes;
  @Singular
  private final List<String> arguments;
  @Builder.Default
  private final ServiceContext.Priority priority = ServiceContext.Priority.standard;

  public enum Type {
    single, resultset
  }

  public static class ServiceRequestMessageBuilder {
    public ServiceRequestMessageBuilder addArgument(@NonNull Class type, String value) {
      setArgumentType(type.getName());
      setArgument(value);
      return this;
    }
  }
}
