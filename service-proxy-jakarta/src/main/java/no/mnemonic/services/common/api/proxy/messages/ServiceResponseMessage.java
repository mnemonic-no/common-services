package no.mnemonic.services.common.api.proxy.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;
import java.util.UUID;

/**
 * Request format for the service message bus
 */
@Builder(setterPrefix = "set")
@Getter
@ToString(onlyExplicitlyIncluded = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@Jacksonized
public class ServiceResponseMessage {

  @ToString.Include
  private UUID requestID;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Map<String, String> metaData;
  private String response;
  private String exception;

}
