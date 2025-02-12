package no.mnemonic.services.common.api.proxy.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

@Builder(setterPrefix = "set")
@Getter
@ToString(onlyExplicitlyIncluded = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExceptionMessage {

  @ToString.Include
  private final String callID;
  private final String exception;
  private final long timestamp;

}
