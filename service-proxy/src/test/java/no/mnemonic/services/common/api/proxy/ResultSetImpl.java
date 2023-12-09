package no.mnemonic.services.common.api.proxy;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import no.mnemonic.services.common.api.ResultSet;

import java.util.Iterator;

@Builder(setterPrefix = "set")
@Getter
@ToString
public class ResultSetImpl<T> implements ResultSet<T>{

    private final int count;
    private final int limit;
    private final int offset;
    private final Iterator<T> iterator;

    @Override
    public Iterator<T> iterator() {
      return iterator;
    }

}
