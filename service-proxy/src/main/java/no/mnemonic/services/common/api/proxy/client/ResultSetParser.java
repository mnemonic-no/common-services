package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.AllArgsConstructor;
import lombok.CustomLog;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.io.InputStream;
import java.util.UUID;

/**
 * ResultSet implementation reading data from a Service Proxy HTTP response
 */
@CustomLog
@AllArgsConstructor
public class ResultSetParser {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  private final Serializer serializer;

  /***
   * Parse the input stream into a streaming resultset.
   *
   * @param response an open response inputstream. This is being closed when the parser is done (also on exceptions)
   * @return a streaming resultset
   * @param <T> the type of the streaming resultset data
   * @throws Exception if the parser reads a service exception, or an exception occurs reading the resultset
   */
  public <T> ClientResultSet<T> parse(InputStream response) throws Exception {
    try {
      JsonParser parser = MAPPER.createParser(response);
      assertThat(parser.nextToken() == JsonToken.START_OBJECT, "Expected start-of-object");

      int count, limit, offset;
      UUID requestID = null;
      count = limit = offset = 0;
      JsonParser data = null;

      //stop looking for new fields when we reach end-object, or when the data array is found
      assertThat(SetUtils.in(parser.nextToken(), JsonToken.FIELD_NAME, JsonToken.END_OBJECT), "Expected field-name or end-of-object");
      while (parser.currentToken() != JsonToken.END_OBJECT) {
        switch (parser.currentName()) {
          case "count":
            assertThat(parser.nextToken() == JsonToken.VALUE_NUMBER_INT, "Expected int-value");
            count = parser.getIntValue();
            assertThat(SetUtils.in(parser.nextToken(), JsonToken.FIELD_NAME, JsonToken.END_OBJECT), "Expected field-name or end-of-object");
            continue;
          case "limit":
            assertThat(parser.nextToken() == JsonToken.VALUE_NUMBER_INT, "Expected int-value");
            limit = parser.getIntValue();
            assertThat(SetUtils.in(parser.nextToken(), JsonToken.FIELD_NAME, JsonToken.END_OBJECT), "Expected field-name or end-of-object");
            continue;
          case "offset":
            assertThat(parser.nextToken() == JsonToken.VALUE_NUMBER_INT, "Expected int-value");
            offset = parser.getIntValue();
            assertThat(SetUtils.in(parser.nextToken(), JsonToken.FIELD_NAME, JsonToken.END_OBJECT), "Expected field-name or end-of-object");
            continue;
          case "exception":
            assertThat(parser.nextToken() == JsonToken.VALUE_STRING, "Expected string-value");
            throw (Exception) serializer.deserializeB64(parser.getText());
          case "data":
            assertThat(parser.nextToken() == JsonToken.START_ARRAY, "Expected start-of-array");
            assertThat(SetUtils.in(parser.nextToken(), JsonToken.END_ARRAY, JsonToken.VALUE_STRING), "Expected end-of-array or value-string");
            data = parser;
            break; //break when hitting data, as this starts the streaming
          default:
            String fieldName = parser.getCurrentName();
            assertThat(parser.nextToken().isScalarValue(), "Expected scalar value");
            assertThat(SetUtils.in(parser.nextToken(), JsonToken.FIELD_NAME, JsonToken.END_OBJECT), "Expected field-name or end-of-object");
            LOGGER.debug("Received unexpected field %s", fieldName);
        }
        if (data != null) break;
      }
      if (data == null) return null;
      return new ClientResultSet<>(serializer, count, limit, offset, data);
    } catch (Exception e) {
      LOGGER.error(e, "Error parsing resultset");
      //make sure response is closed if parser stops processing here
      LambdaUtils.tryTo(response::close, ex->LOGGER.error(ex, "Error closing response"));
      throw e;
    }
  }

  private void assertThat(boolean b, String reason) {
    if (!b) throw new IllegalStateException("Error parsing JSON: " + reason);
  }

}
