package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AllArgsConstructor;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.services.common.api.ServiceContext;
import no.mnemonic.services.common.api.proxy.Utils;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;
import no.mnemonic.services.common.api.proxy.messages.ServiceResponseMessage;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.UUID;

import static no.mnemonic.services.common.api.proxy.Utils.fromTypes;

@AllArgsConstructor
@CustomLog
public class ServiceMessageConverter {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  @NonNull
  private final Serializer serializer;

  /**
   * Convert a request into a serializable request message
   * @param requestID the requestID of the request
   * @param method the method being invoked
   * @param arguments the (serializable) arguments to pass with the request
   * @param priority the priority to set on the request
   * @return the prepared ServiceRequestMessage
   * @throws IOException if serialization of the arguments fails
   */
  public ServiceRequestMessage convert(UUID requestID,
                                       Method method,
                                       Object[] arguments,
                                       ServiceContext.Priority priority
  ) throws IOException {
    return ServiceRequestMessage.builder()
        .setRequestID(requestID)
        .setPriority(priority)
        .setArgumentTypes(fromTypes(method.getParameterTypes()))
        .setSerializerID(serializer.serializerID())
        .setArguments(Utils.serialize(serializer, arguments))
        .build();
  }


  /**
   * Reads the response message from the HTTP response
   * @param response the HTTP response
   * @return the deserialized response message
   * @throws Exception the deserialized exception, if the response contained an exception
   */
  public Object readResponseMessage(InputStream response) throws Exception {
    ServiceResponseMessage responseMessage = MAPPER.readValue(response, ServiceResponseMessage.class);
    if (responseMessage.getException() != null) {
      if (LOGGER.isDebug()) LOGGER.debug(">> received exception [callID=%s]", responseMessage.getRequestID());
      throw serializer.<Exception>deserializeB64(responseMessage.getException());
    }

    if (LOGGER.isDebug()) {
      LOGGER.debug(">> received response [callID=%s]", responseMessage.getRequestID());
    }
    if (responseMessage.getResponse() == null) return null;
    return serializer.deserializeB64(responseMessage.getResponse());
  }

  //private methods


}
