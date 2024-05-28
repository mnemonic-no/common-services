package no.mnemonic.services.common.api.proxy.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import lombok.CustomLog;
import lombok.RequiredArgsConstructor;
import no.mnemonic.commons.utilities.collections.SetUtils;
import no.mnemonic.services.common.api.Resource;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

@RequiredArgsConstructor
@CustomLog
public class ClientResultSet<T> implements ResultSet<T> {

  private final Serializer serializer;
  private final int count;
  private final int limit;
  private final int offset;
  private final JsonParser data;
  private final Resource resource;

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
    return new ClientResultSetIterator<>();
  }

  @Override
  public void close() {
    if (closed) return;
    try {
      if (resource != null) {
        tryTo(resource::close, e -> LOGGER.error(e, "Error closing resource"));
      }
      data.close();
      closed = true;
    } catch (IOException e) {
      throw new IllegalStateException("Error closing stream", e);
    }
  }

  @Override
  public void cancel() {
    if (closed) return;
    try {
      if (resource != null) {
        tryTo(resource::cancel, e -> LOGGER.error(e, "Error cancelling resource"));
      }
      data.close();
      closed = true;
    } catch (IOException e) {
      throw new IllegalStateException("Error cancelling stream", e);
    }
  }

  private void assertThat(boolean b, String reason) {
    if (!b) throw new IllegalStateException("Error parsing JSON: " + reason);
  }

  private class ClientResultSetIterator<T> implements Iterator<T>, Closeable {

    @Override
    public void close() {
      ClientResultSet.this.close();
    }

    @Override
    public boolean hasNext() {
      if (closed) return false;
      if (data.currentToken() == JsonToken.END_ARRAY) {
        this.close();
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
  }
}
