package no.mnemonic.services.common.api.proxy;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.annotations.ResultBatchSize;
import no.mnemonic.services.common.api.annotations.ResultSetExtention;

import java.util.Iterator;
import java.util.function.Function;

public interface TestService extends Service {

  String getString(String arg) throws TestException;

  String primitiveLongArgument(long arg) throws TestException;

  String primitiveIntArgument(int arg) throws TestException;

  String primitiveCharArgument(char arg) throws TestException;

  String primitiveByteArgument(byte arg) throws TestException;

  String primitiveFloatArgument(float arg) throws TestException;

  String primitiveDoubleArgument(double arg) throws TestException;

  String primitiveBooleanArgument(boolean arg) throws TestException;

  String objectArrayArgument(String[] arg) throws TestException;

  String customObjectArgument(TestArgument arg) throws TestException;

  String primitiveArrayArgument(long[] arg) throws TestException;

  ResultSet<String> getResultSet(String arg) throws TestException;

  @ResultBatchSize(1)
  ResultSet<String> getResultSetWithBatchSize(String arg) throws TestException;

  MyResultSet<String> getMyResultSet(String arg) throws TestException;

  MyAnnotatedResultSet<String> getMyAnnotatedResultSet(String arg) throws TestException;

  abstract class MyResultSet<T> implements ResultSet<T> {
  }

  @Builder(setterPrefix = "set")
  @Getter
  @ToString
  @ResultSetExtention(extender = MyResultSetExtender.class)
  class MyAnnotatedResultSet<T> implements ResultSet<T> {
    private final int count;
    private final int limit;
    private final int offset;
    private final Iterator<T> iterator;

    @Override
    public Iterator<T> iterator() {
      return iterator;
    }
  }

  class MyResultSetExtender implements Function<ResultSet<?>, MyAnnotatedResultSet<?>> {
    @Override
    public MyAnnotatedResultSet<?> apply(ResultSet<?> rs) {
      return MyAnnotatedResultSet.<Object>builder()
              .setIterator((Iterator<Object>) rs.iterator())
              .setCount(rs.getCount())
              .setOffset(rs.getOffset())
              .setLimit(rs.getLimit())
              .build();
    }
  }
}
