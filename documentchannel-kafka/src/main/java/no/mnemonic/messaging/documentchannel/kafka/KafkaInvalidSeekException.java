package no.mnemonic.messaging.documentchannel.kafka;

/**
 * Exception thrown if attempting to seek to an invalid offset in Kafka
 */
public class KafkaInvalidSeekException extends Exception {

  private static final long serialVersionUID = 4620105297163607882L;

  public KafkaInvalidSeekException(String msg) {
    super(msg);
  }
}
