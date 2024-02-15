package no.mnemonic.services.common.api.proxy;

import no.mnemonic.commons.utilities.collections.ListUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UtilsTest {
  @Test
  void toTypes() throws ClassNotFoundException {
    assertArrayEquals(
            new Class[]{String.class},
            Utils.toTypes(ListUtils.list("java.lang.String"))
    );
  }

  @Test
  void fromTypes() throws ClassNotFoundException {
    assertEquals(
            ListUtils.list("java.lang.String"),
            Utils.fromTypes(new Class[]{String.class})
    );
  }

  @Test
  void toTypesHandlesPrimitiveTypes() throws ClassNotFoundException {
    assertArrayEquals(
            new Class[]{long.class, int.class, boolean.class, char.class, float.class, double.class, short.class, byte.class},
            Utils.toTypes(ListUtils.list("long", "int", "boolean", "char", "float", "double", "short", "byte"))
    );
  }

  @Test
  void fromTypesHandlesPrimitiveTypes() throws ClassNotFoundException {
    assertEquals(
            ListUtils.list("long", "int", "boolean", "char", "float", "double", "short", "byte"),
            Utils.fromTypes(new Class[]{long.class, int.class, boolean.class, char.class, float.class, double.class, short.class, byte.class})
    );
  }
}