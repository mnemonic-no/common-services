package no.mnemonic.services.common.messagebus;

import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.services.common.api.ResultSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

abstract class AbstractServiceMessageTest {

  static Iterator<String> createResults(int size) {
    Collection<String> values = new ArrayList<>();
    for (int i = 0; i < size; i++) values.add("val" + i);
    return values.iterator();
  }

  static ResultSet<String> createResultSet(Iterator<String> iterator) {
    return new ResultSetImpl<>(1000, 100, 10, iterator);
  }

  public static class ResultSetImpl<T> implements ResultSet<T>, Serializable, AppendMembers {

    private final int count;
    private final int limit;
    private final int offset;
    private final Iterator<T> iterator;

    @SuppressWarnings("WeakerAccess")
    public ResultSetImpl(int count, int limit, int offset, Iterator<T> iterator) {
      this.count = count;
      this.limit = limit;
      this.offset = offset;
      this.iterator = iterator;
    }

    @Override
    public void appendMembers(StringBuilder buf) {
      AppendUtils.appendField(buf, "count", count);
      AppendUtils.appendField(buf, "limit", limit);
      AppendUtils.appendField(buf, "offset", offset);
    }

    @Override
    public String toString() {
      return AppendUtils.toString(this);
    }

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
      return iterator;
    }
  }

}
