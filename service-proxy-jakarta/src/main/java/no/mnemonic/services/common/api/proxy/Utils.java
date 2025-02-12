package no.mnemonic.services.common.api.proxy;

import lombok.NonNull;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.services.common.api.proxy.serializer.Serializer;

import java.io.IOException;
import java.util.List;

public class Utils {
  public static final int HTTP_ERROR_RESPONSE = 500;
  public static final int HTTP_OK_RESPONSE = 200;

  public static List<String> serialize(@NonNull Serializer serializer, @NonNull Object[] arguments) throws IOException {
    List<String> serialized = ListUtils.list();
    for (int i = 0; i < arguments.length; i++) {
      serialized.add(serializer.serializeB64(arguments[i]));
    }
    return serialized;
  }

  public static Object[] toArgs(@NonNull Serializer serializer, @NonNull List<String> arguments) throws IOException {
    Object[] args = new Object[arguments.size()];
    for (int i = 0; i < arguments.size(); i++) {
      args[i] = serializer.deserializeB64(arguments.get(i));
    }
    return args;
  }

  public static List<String> fromTypes(@NonNull Class<?>[] types) {
    List<String> clz = ListUtils.list();
    for (int i = 0; i < types.length; i++) {
      clz.add(types[i].getName());
    }
    return clz;
  }

  public static Class<?>[] toTypes(@NonNull List<String> types) throws ClassNotFoundException {
    Class<?>[] clz = new Class[types.size()];
    for (int i = 0; i < types.size(); i++) {
      clz[i] = toType(types.get(i));
    }
    return clz;
  }

  private static Class<?> toType(String className) throws ClassNotFoundException {
    if (className == null) throw new ClassNotFoundException("No type: null");
    switch (className) {
      case "long":
        return long.class;
      case "float":
        return float.class;
      case "int":
        return int.class;
      case "boolean":
        return boolean.class;
      case "char":
        return char.class;
      case "double":
        return double.class;
      case "byte":
        return byte.class;
      case "short":
        return short.class;
      default:
        return Class.forName(className);
    }
  }
}
