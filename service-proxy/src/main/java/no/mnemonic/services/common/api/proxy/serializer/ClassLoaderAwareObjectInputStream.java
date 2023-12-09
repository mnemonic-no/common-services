package no.mnemonic.services.common.api.proxy.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class ClassLoaderAwareObjectInputStream extends ObjectInputStream {

  private final ClassLoader classLoader;

  public ClassLoaderAwareObjectInputStream(InputStream in, ClassLoader cl) throws IOException {
    super(in);
    this.classLoader = cl;
  }

  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    if (desc == null) throw new IllegalArgumentException("ObjectStreamClass not set");
    return Class.forName(desc.getName(), false, classLoader);
  }

}
