package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaCursorTest  {

  private final ConsumerRecord<String, String> recordT1P0 = new ConsumerRecord<>("topic1", 0, 10, "key", "value");
  private final ConsumerRecord<String, String> recordT1P1 = new ConsumerRecord<>("topic1", 1, 20, "key", "value");
  private final ConsumerRecord<String, String> recordT2P0 = new ConsumerRecord<>("topic2", 0, 30, "key", "value");
  private final ConsumerRecord<String, String> recordT2P1 = new ConsumerRecord<>("topic2", 1, 40, "key", "value");
  private final KafkaCursor cursor = new KafkaCursor();

  @BeforeEach
  public void setUp() {
    cursor.register(recordT1P0);
    cursor.register(recordT1P1);
    cursor.register(recordT2P0);
    cursor.register(recordT2P1);
  }

  @Test
  void testSerialize() {
    String stringCursor = cursor.toString();

    KafkaCursor deserialized = KafkaCursor.valueOf(stringCursor);
    assertEquals(2, deserialized.getPointers().size());

    assertPointer(deserialized, recordT1P0);
    assertPointer(deserialized, recordT1P1);
    assertPointer(deserialized, recordT2P0);
    assertPointer(deserialized, recordT2P1);
  }

  private void assertPointer(KafkaCursor deserialized, ConsumerRecord<String, String> rec) {
    assertEquals(rec.offset(), deserialized.getPointers().get(rec.topic()).get((rec.partition())).getOffset());
    assertEquals(rec.timestamp(), deserialized.getPointers().get(rec.topic()).get((rec.partition())).getTimestamp());
  }
}