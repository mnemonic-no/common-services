package no.mnemonic.services.common.api.proxy.serializer;

import lombok.NonNull;
import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.metrics.TimerContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.LongAdder;

public class DefaultJavaSerializer implements Serializer {

  private static final Logger LOGGER = Logging.getLogger(DefaultJavaSerializer.class);
  private static final String SERIALIZER_ID = "JSER";

  private final LongAdder serializeCount = new LongAdder();
  private final LongAdder serializeError = new LongAdder();
  private final LongAdder serializeTime = new LongAdder();
  private final LongAdder serializeMsgSize = new LongAdder();
  private final LongAdder deserializeCount = new LongAdder();
  private final LongAdder deserializeError = new LongAdder();
  private final LongAdder deserializeTime = new LongAdder();
  private final LongAdder deserializeMsgSize = new LongAdder();

  @Override
  public String serializerID() {
    return SERIALIZER_ID;
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
            .addData("deserializeTime", deserializeTime)
            .addData("deserializeMsgSize", deserializeMsgSize);
  }

  @Override
  public byte[] serialize(@NonNull Object msg) throws IOException {
    serializeCount.increment();

    try (
            TimerContext ignored = TimerContext.timerMillis(serializeTime::add);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)
    ) {
      oos.writeObject(msg);
      serializeMsgSize.add(baos.size());
      LOGGER.debug("JSER serialize size=%d", baos.size());
      return baos.toByteArray();
    } catch (Exception e) {
      serializeError.increment();
      LOGGER.error(e, "Error in serialize");
      throw new IOException("Error in serialize", e);
    }
  }

  @Override
  public <T> T deserialize(@NonNull byte[] msgbytes, @NonNull ClassLoader classLoader) throws IOException {
    deserializeCount.increment();
    deserializeMsgSize.add(msgbytes.length);
    LOGGER.debug("JSER deserialize size=%d", msgbytes.length);

    try (
            TimerContext ignored = TimerContext.timerMillis(deserializeTime::add);
            ObjectInputStream ois = new ClassLoaderAwareObjectInputStream(new ByteArrayInputStream(msgbytes), classLoader)
    ) {
      //noinspection unchecked
      return (T) ois.readObject();
    } catch (Exception e) {
      deserializeError.increment();
      LOGGER.error(e, "Error in deserialize");
      throw new IOException("Error in deserialize", e);
    }
  }
}
