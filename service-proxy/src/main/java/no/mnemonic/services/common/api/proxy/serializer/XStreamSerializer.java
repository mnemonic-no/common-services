package no.mnemonic.services.common.api.proxy.serializer;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.enums.EnumConverter;
import com.thoughtworks.xstream.io.HierarchicalStreamDriver;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.xml.MXParserDriver;
import com.thoughtworks.xstream.security.ForbiddenClassException;
import com.thoughtworks.xstream.security.NoTypePermission;
import com.thoughtworks.xstream.security.NullPermission;
import com.thoughtworks.xstream.security.PrimitiveTypePermission;
import lombok.Builder;
import lombok.Singular;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;
import no.mnemonic.services.common.api.proxy.messages.ExceptionMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.MapUtils.map;

@Builder(setterPrefix = "set", buildMethodName = "_buildInternal")
public class XStreamSerializer implements Serializer {

  private static final Logger LOGGER = Logging.getLogger(XStreamSerializer.class);
  private static final String DEFAULT_SERIALIZER_ID = "XSTREAM";

  @Builder.Default
  private final HierarchicalStreamDriver driver = new MXParserDriver();
  @Builder.Default
  private final String serializerID = DEFAULT_SERIALIZER_ID;

  private final boolean disableReferences;
  private final boolean ignoreUnknownEnumLiterals;
  private final Consumer<XStream> decodingXstreamCustomizer;
  private final Consumer<XStream> encodingXstreamCustomizer;

  @Singular
  private final Set<Class> allowedClasses;
  @Singular
  private final Set<String> allowedClassesRegexes;
  @Singular
  private final Map<String, Class> typeAliases;
  @Singular
  private final Map<String, String> packageAliases;
  @Singular
  private final Map<String, Class> decodingTypeAliases;
  @Singular
  private final Map<String, String> decodingPackageAliases;

  private final AtomicReference<XStream> decodingXstream = new AtomicReference<>();
  private final AtomicReference<XStream> encodingXstream = new AtomicReference<>();

  private final LongAdder serializeCount = new LongAdder();
  private final LongAdder serializeError = new LongAdder();
  private final LongAdder serializeTime = new LongAdder();
  private final LongAdder serializeMsgSize = new LongAdder();
  private final LongAdder deserializeCount = new LongAdder();
  private final LongAdder deserializeError = new LongAdder();
  private final LongAdder deserializeForbiddenClassError = new LongAdder();
  private final LongAdder deserializeInvalidElement = new LongAdder();
  private final LongAdder deserializeTime = new LongAdder();
  private final LongAdder deserializeMsgSize = new LongAdder();


  private XStreamSerializer init() {
    XStream decodingXstream = new XStream(driver);
    XStream encodingXstream = new XStream(driver);

    if (disableReferences) {
      encodingXstream.setMode(XStream.NO_REFERENCES);
    }
    if (ignoreUnknownEnumLiterals) {
      decodingXstream.registerConverter(new CompatibilityEnumConverter());
    }

    decodingXstream.addPermission(NoTypePermission.NONE);
    decodingXstream.addPermission(PrimitiveTypePermission.PRIMITIVES);
    decodingXstream.addPermission(NullPermission.NULL);
    decodingXstream.allowTypesByRegExp(list(allowedClassesRegexes).toArray(new String[]{}));
    decodingXstream.allowTypes(list(allowedClasses).toArray(new Class[]{}));
    decodingXstream.ignoreUnknownElements();

    //allow types needed for proper error handling
    decodingXstream.allowTypes(new Class[]{
        String.class,
        List.class,
        StackTraceElement.class,
        IllegalDeserializationException.class,
        ExceptionMessage.class
    });
    decodingXstream.allowTypesByRegExp(new String[]{"java.util.Collections\\$UnmodifiableList"});
    decodingXstream.allowTypesByRegExp(new String[]{"java.util.Collections\\$EmptyList"});

    map(packageAliases).forEach((a, p) -> {
      decodingXstream.aliasPackage(a, p);
      encodingXstream.aliasPackage(a, p);
    });
    map(typeAliases).forEach((a, c) -> {
      decodingXstream.alias(a, c);
      encodingXstream.alias(a, c);
    });
    map(decodingPackageAliases).forEach(decodingXstream::aliasPackage);
    map(decodingTypeAliases).forEach(decodingXstream::alias);

    //allow non-standard customization
    if (decodingXstreamCustomizer != null) decodingXstreamCustomizer.accept(decodingXstream);
    if (encodingXstreamCustomizer != null) encodingXstreamCustomizer.accept(encodingXstream);

    this.encodingXstream.set(encodingXstream);
    this.decodingXstream.set(decodingXstream);
    return this;
  }

  @Override
  public String serializerID() {
    return serializerID;
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    return new MetricsData()
        .addData("serializeCount", serializeCount)
        .addData("serializeError", serializeError)
        .addData("serializeTime", serializeTime)
        .addData("serializeMsgSize", serializeMsgSize)
        .addData("deserializeCount", deserializeCount)
        .addData("deserializeError", deserializeError)
        .addData("deserializeForbiddenClassError", deserializeForbiddenClassError)
        .addData("deserializeInvalidElement", deserializeInvalidElement)
        .addData("deserializeTime", deserializeTime)
        .addData("deserializeMsgSize", deserializeMsgSize);
  }

  @Override
  public byte[] serialize(Object msg) throws IOException {
    serializeCount.increment();

    try (
        TimerContext ignored = TimerContext.timerMillis(serializeTime::add);
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
    ) {
      encodingXstream.get().marshal(msg, driver.createWriter(baos));
      serializeMsgSize.add(baos.size());
      if (LOGGER.isDebug()) {
        LOGGER.debug("XStream serialize driver=%s size=%d", driver.getClass(), baos.size());
      }
      return baos.toByteArray();
    } catch (Exception e) {
      serializeError.increment();
      LOGGER.error(e, "Error in serialize");
      throw new IOException("Error in serialize", e);
    }
  }

  @Override
  public <T> T deserialize(byte[] msgbytes, ClassLoader classLoader) throws IOException {
    deserializeCount.increment();
    deserializeMsgSize.add(msgbytes.length);
    if (LOGGER.isDebug()) {
      LOGGER.debug("XStream deserialize driver=%s size=%d", driver.getClass(), msgbytes.length);
    }

    try (
        TimerContext ignored = TimerContext.timerMillis(deserializeTime::add);
        ByteArrayInputStream bais = new ByteArrayInputStream(msgbytes)
    ) {
      //noinspection unchecked
      return (T) decodingXstream.get().unmarshal(driver.createReader(bais));
    } catch (ForbiddenClassException e) {
      deserializeForbiddenClassError.increment();
      LOGGER.error(e, "Forbidden class in deserialize");
      throw new IllegalDeserializationException(e.getMessage());
    } catch (Throwable e) {
      if (e.getCause() instanceof ForbiddenClassException) {
        deserializeForbiddenClassError.increment();
        LOGGER.error(e, "Forbidden class in deserialize");
        throw new IllegalDeserializationException(e.getMessage());
      } else {
        deserializeError.increment();
        LOGGER.error(e, "Error in deserialize");
        throw new IOException("Error in deserialize", e);
      }
    }
  }

  private class CompatibilityEnumConverter extends EnumConverter {
    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
      try {
        return super.unmarshal(reader, context);
      } catch (IllegalArgumentException e) {
        deserializeInvalidElement.increment();
        LOGGER.warning(e, "Ignoring invalid enum literal, returning null!");
        return null;
      }
    }
  }

  public static class XStreamSerializerBuilder {
    public XStreamSerializer build() {
      return _buildInternal().init();
    }
  }

}
