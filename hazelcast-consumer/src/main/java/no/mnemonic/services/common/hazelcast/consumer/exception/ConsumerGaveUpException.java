package no.mnemonic.services.common.hazelcast.consumer.exception;

/**
 * This exception is used in HazelcastTransactionalConsumerHandler and will cause the transactional consumer thread
 * to roll back the transaction and die. Overriding the keepThreadAliveOnException-setting.
 * It's intention is to provide the TransactionalConsumer with the tools to implement their own conditional retry mechanism,
 * should the transaction time out or something unexpected happen in the TransactionalConsumer implementation.
 */
public class ConsumerGaveUpException extends Exception {
  private static final long serialVersionUID = 5198270553165820889L;

  public ConsumerGaveUpException(Throwable cause) {
    super(cause);
  }

  public ConsumerGaveUpException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConsumerGaveUpException(String message) {
    super(message);
  }
}
