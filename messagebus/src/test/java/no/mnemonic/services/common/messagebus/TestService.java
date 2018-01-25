package no.mnemonic.services.common.messagebus;

import no.mnemonic.services.common.api.ResultSet;
import no.mnemonic.services.common.api.ResultSetStreamInterruptedException;
import no.mnemonic.services.common.api.Service;
import no.mnemonic.services.common.api.annotations.ResultBatchSize;
import no.mnemonic.services.common.api.annotations.ResultSetExtention;

import java.util.Iterator;
import java.util.function.Function;

public interface TestService extends Service {

  String getString(String arg);

  String primitiveLongArgument(long arg);

  String primitiveIntArgument(int arg);

  String primitiveCharArgument(char arg);

  String primitiveByteArgument(byte arg);

  String primitiveFloatArgument(float arg);

  String primitiveDoubleArgument(double arg);

  String primitiveBooleanArgument(boolean arg);

  String objectArrayArgument(String[] arg);

  String primitiveArrayArgument(long[] arg);

  ResultSet<String> getResultSet(String arg);

  @ResultBatchSize(1)
  ResultSet<String> getResultSetWithBatchSize(String arg);

  MyResultSet<String> getMyResultSet(String arg);

  MyAnnotatedResultSet<String> getMyAnnotatedResultSet(String arg);

  abstract class MyResultSet<T> implements ResultSet<T> {
  }

  @ResultSetExtention(extender = MyResultSetExtender.class)
  abstract class MyAnnotatedResultSet<T> implements ResultSet<T> {
  }

  class MyResultSetExtender implements Function<ResultSet, MyAnnotatedResultSet> {
    @Override
    public MyAnnotatedResultSet apply(ResultSet rs) {
      return new MyAnnotatedResultSet() {
        @Override
        public int getCount() {
          return rs.getCount();
        }

        @Override
        public int getLimit() {
          return rs.getLimit();
        }

        @Override
        public int getOffset() {
          return rs.getOffset();
        }

        @Override
        public Iterator iterator() throws ResultSetStreamInterruptedException {
          return rs.iterator();
        }
      };
    }
  }
}
