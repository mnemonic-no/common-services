package no.mnemonic.messaging.documentchannel.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.json.JsonMapper;
import no.mnemonic.commons.utilities.StringUtils;
import no.mnemonic.commons.utilities.collections.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static no.mnemonic.commons.utilities.collections.MapUtils.map;

/**
 * This is a private class representing a Kafka cursor.
 * The information in this cursor should only be parsed and understood by the KafkaDocumentSource.
 * Clients should not attempt to create/parse this cursor, as the format may change.
 */
class KafkaCursor {

  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final ObjectReader READER = MAPPER.readerFor(KafkaCursor.class);
  private static final ObjectWriter WRITER = MAPPER.writerFor(KafkaCursor.class);
  private static final Base64.Decoder B64_DECODER = Base64.getDecoder();
  private static final Base64.Encoder B64_ENCODER = Base64.getEncoder();

  /*
   * Pointers map contains one entry per topic (topicName->partitionMap) which is consumed by the current consumer.
   * Each partition map contains one entry per partition with the offset and timestamp of the last consumed message from this partition.
   * Example
   *   Topic.DC1 -> (0,{offset:10,timestamp:12345678}), (1,{offset,20,timestamp:12341234})
   *   Topic.DC2 -> (0,{offset:15,timestamp:12344321}), (1,{offset,25,timestamp:12347654})
   */
  private final Map<String, Map<Integer, OffsetAndTimestamp>> pointers;

  public KafkaCursor() {
    pointers = new ConcurrentHashMap<>();
  }

  @JsonCreator
  public KafkaCursor(@JsonProperty("pointers") Map<String, Map<Integer, OffsetAndTimestamp>> pointers) {
    this.pointers = new ConcurrentHashMap<>(map(pointers));
  }

  public KafkaCursor(KafkaCursor original) {
    //deep copy of the cursor
    this.pointers = new ConcurrentHashMap<>(map(
            original.pointers.entrySet(),
            entry -> MapUtils.pair(entry.getKey(), new ConcurrentHashMap<>(map(entry.getValue())))
    ));
  }

  public Map<String, Map<Integer, OffsetAndTimestamp>> getPointers() {
    return pointers;
  }

  @Override
  public String toString() {
    try {
      return B64_ENCODER.encodeToString(WRITER.writeValueAsBytes(this));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Error JSON-encoding cursor", e);
    }
  }

  /**
   * Update this pointer to reflect the position of this record
   * @param record the received record to update the pointer to
   */
  void register(ConsumerRecord<?, ?> record) {
    if (record == null) throw new IllegalArgumentException("record was null");
    pointers.computeIfAbsent(record.topic(), t -> new ConcurrentHashMap<>()).put(
            record.partition(),
            new OffsetAndTimestamp(record.offset(), record.timestamp())
    );
  }

  /**
   * Set the state of this pointer to match the current position of the current consumer (only for assigned partitions)
   * @param consumer the consumer to set pointer to. The consumer must be assigned.
   */
  void set(KafkaConsumer<?, ?> consumer) {
    if (consumer == null) throw new IllegalArgumentException("consumer was null");
    pointers.clear();
    consumer.endOffsets(consumer.assignment()).forEach((tp, pos) -> {
      pointers.computeIfAbsent(tp.topic(), t->new ConcurrentHashMap<>()).put(tp.partition(), new OffsetAndTimestamp(pos, 0));
    });
  }

  void parse(String cursor) {
    if (StringUtils.isBlank(cursor)) return;
    try {
      KafkaCursor input = valueOf(cursor);
      pointers.putAll(input.pointers);
    } catch (Exception e) {
      throw new IllegalStateException("Error decoding cursor", e);
    }
  }

  static KafkaCursor valueOf(String cursor) {
    if (cursor == null) throw new IllegalArgumentException("cursor was null");
    try {
      byte[] json = B64_DECODER.decode(cursor);
      return READER.readValue(json);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid cursor: " + cursor);
    }
  }

  static class OffsetAndTimestamp {
    private final long offset;
    private final long timestamp;

    @JsonCreator
    public OffsetAndTimestamp(
            @JsonProperty("offset") long offset,
            @JsonProperty("timestamp") long timestamp
    ) {
      this.offset = offset;
      this.timestamp = timestamp;
    }

    public long getOffset() {
      return offset;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}
