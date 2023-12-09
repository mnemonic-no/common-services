package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.RequiredArgsConstructor;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.io.IOException;
import java.util.Iterator;

@RequiredArgsConstructor
public class ClientResultSet<T> implements ResultSet<T> {

  private final Serializer serializer;
  private final int count;
  private final int limit;
  private final int offset;
  private final JsonParser data;

  private boolean closed;

  @Override
  public int getCount() {
    return count;
  }

  @Override
  public int getLimit() {
    return limit;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        if (closed) return false;
        if (data.currentToken() == JsonToken.END_ARRAY) {
          close();
          return false;
        } else {
          return true;
        }
      }

      @Override
      public T next() {
        try {
          //expect the parser to be advanced to an array element already
          String value = data.getText();
          //advance and verify that next token is also an element, or end-of-array
          assertThat(SetUtils.in(data.nextToken(), JsonToken.END_ARRAY, JsonToken.VALUE_STRING), "Expected end-of-array or value-string");
          //deserialize the previous element
          return serializer.deserializeB64(value);
        } catch (IOException e) {
          throw new IllegalStateException("Error deserializing resultset value", e);
        }
      }
    };
  }

  @Override
  public void close() {
    try {
      data.close();
      closed = true;
    } catch (IOException e) {
      throw new IllegalStateException("Error closing stream", e);
    }
  }

  private void assertThat(boolean b, String reason) {
    if (!b) throw new IllegalStateException("Error parsing JSON: " + reason);
  }
}
