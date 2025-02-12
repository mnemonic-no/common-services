package no.mnemonic.services.common.api.proxy;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetExtender;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.annotations.ResultBatchSize;
import no.mnemonic.services.common.api.annotations.ResultSetExtention;

import java.util.Iterator;
import java.util.Map;

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

  MyExtendedResultSet<String> getMyExtendedResultSet(String arg) throws TestException;

  abstract class MyResultSet<T> implements ResultSet<T> {
  }

  @Builder(setterPrefix = "set")
  @Getter
  @ToString
  @ResultSetExtention(extender = MyAnnotatedResultSetExtender.class)
  class MyAnnotatedResultSet<T> implements ResultSet<T> {
    private final int count;
    private final int limit;
    private final int offset;
    private final Iterator<T> iterator;
    private final String token;

    @Override
    public Iterator<T> iterator() {
      return iterator;
    }
  }

  @Builder(setterPrefix = "set")
  @Getter
  @ToString
  class MyExtendedResultSet<T> implements ResultSet<T> {
    private final int count;
    private final int limit;
    private final int offset;
    private final Iterator<T> iterator;
    private final String metaKey;

    @Override
    public Iterator<T> iterator() {
      return iterator;
    }
  }

  class MyAnnotatedResultSetExtender implements ResultSetExtender<MyAnnotatedResultSet> {

    public static final String METADATA_KEY = "token";

    @Override
    public MyAnnotatedResultSet extend(ResultSet rs, Map<String, String> metaData) {
      return MyAnnotatedResultSet.builder()
              .setIterator((Iterator<Object>) rs.iterator())
              .setCount(rs.getCount())
              .setOffset(rs.getOffset())
              .setLimit(rs.getLimit())
              .setToken(metaData.get(METADATA_KEY))
              .build();
    }

    @Override
    public Map<String, String> extract(MyAnnotatedResultSet extendedResultSet) {
      return MapUtils.map(MapUtils.pair(METADATA_KEY, extendedResultSet.getToken()));
    }

  }

  class MyExtendedResultSetExtender implements ResultSetExtender<MyExtendedResultSet> {

    public static final String METADATA_KEY = "metaKey";

    @Override
    public MyExtendedResultSet extend(ResultSet rs, Map<String, String> metaData) {
      return MyExtendedResultSet.<Object>builder()
              .setIterator(rs.iterator())
              .setCount(rs.getCount())
              .setOffset(rs.getOffset())
              .setLimit(rs.getLimit())
              .setMetaKey(metaData.get(METADATA_KEY))
              .build();
    }

  }
}
