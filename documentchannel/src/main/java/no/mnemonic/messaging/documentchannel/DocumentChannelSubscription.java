package no.mnemonic.messaging.documentchannel;

/**
 * Represents an active subscription to a document channel.
 * An active subscription will submit incoming documents to the attached {@link DocumentChannelListener}
 */
public interface DocumentChannelSubscription {

  /**
   * Cancel the subscription
   */
  void cancel();

}
