package no.mnemonic.services.common.api.proxy.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.logging.LocalLoggingContext;
import no.mnemonic.services.common.api.proxy.Utils;
import no.mnemonic.services.common.api.proxy.client.ServiceClient;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Builder(setterPrefix = "set")
@CustomLog
public class ServiceV1Servlet extends HttpServlet {

  private static final String LOG_KEY_PRIORITY = "priority";
  private static final Pattern INVOCATION_URL_PATTERN = Pattern.compile("/service/v1/(.+)/(single|resultset)/(.+)");

  @NonNull
  private final Map<String, ServiceInvocationHandler<?>> invocationHandlers;
  @NonNull
  private final ObjectMapper mapper;

  private enum Type {
    single, resultset
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    try {
      //determine invocation type and method
      Matcher matcher = INVOCATION_URL_PATTERN.matcher(req.getRequestURI());
      if (!matcher.matches()) {
        LOGGER.error("Request does not match expected URI pattern: %s", req.getRequestURI());
        throw new IllegalArgumentException("Request does not match expected URI pattern: " + req.getRequestURI());
      }
      if (LOGGER.isDebug()) LOGGER.debug("<< request %s", req.getRequestURI());

      String serviceName = URLDecoder.decode(matcher.group(1), StandardCharsets.UTF_8);
      Type type = Type.valueOf(matcher.group(2));
      String methodName = matcher.group(3);


      ServiceInvocationHandler<?> handler = invocationHandlers.get(serviceName);
      if (handler == null) {
        throw new IllegalArgumentException("Service not known: " + serviceName);
      }

      //decode JSON request
      ServiceRequestMessage request = mapper.readValue(req.getInputStream(), ServiceRequestMessage.class);
      try (LocalLoggingContext ignored = LocalLoggingContext.create().using(LOG_KEY_PRIORITY, String.valueOf(request.getPriority()))) {
        if (type == Type.single) {
          handler.handleSingle(methodName, request, resp);
        } else {
          handler.handleStreaming(methodName, request, resp);
        }
      }

    } catch (Exception e) {
      LOGGER.error(e, "Error invoking method");
      resp.setStatus(Utils.HTTP_ERROR_RESPONSE);
    } finally {
      //ensure all downstream clients are closed when this thread is done
      ServiceClient.closeThreadResources();
    }
    resp.flushBuffer();
  }

}
