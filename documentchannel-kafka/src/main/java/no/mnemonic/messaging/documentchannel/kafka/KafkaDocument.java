package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNull;

/**
 * A transport object representing a document coming from Kafka
 *
 * @param <T> document type
 */
public class KafkaDocument<T> {

  private final ConsumerRecord<String, T> rec;
  private final String cursor;

  public KafkaDocument(ConsumerRecord<String, T> rec, String cursor) {
    this.rec = rec;
    this.cursor = cursor;
  }

  public T getDocument() {
    return ifNotNull(rec, ConsumerRecord::value);
  }

  public ConsumerRecord<String, T> getRecord() {
    return rec;
  }

  public String getCursor() {
    return cursor;
  }
}
