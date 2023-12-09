package no.mnemonic.services.common.api.proxy.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.Builder;
import lombok.CustomLog;
import lombok.NonNull;
import no.mnemonic.commons.logging.LocalLoggingContext;
import no.mnemonic.services.common.api.proxy.Utils;
import no.mnemonic.services.common.api.proxy.messages.ServiceRequestMessage;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Builder(setterPrefix = "set")
@CustomLog
public class ServiceV1Servlet extends HttpServlet {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final String LOG_KEY_PRIORITY = "priority";
  private static final Pattern INVOCATION_URL_PATTERN = Pattern.compile("/service/v1/(.+)/(single|resultset)/(.+)");

  @NonNull
  private final Map<String, ServiceInvocationHandler<?>> invocationHandlers;

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

      String serviceName = matcher.group(1);
      Type type = Type.valueOf(matcher.group(2));
      String methodName = matcher.group(3);

      ServiceInvocationHandler<?> handler = invocationHandlers.get(serviceName);
      if (handler == null) {
        throw new IllegalArgumentException("Service not known: " + serviceName);
      }

      //decode JSON request
      ServiceRequestMessage request = MAPPER.readValue(req.getInputStream(), ServiceRequestMessage.class);
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
    }
    resp.flushBuffer();
  }

}
