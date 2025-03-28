package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.admin.ScramMechanism;

public enum SaslMechanism {
  NONE(null), SCRAM_SHA_256(ScramMechanism.SCRAM_SHA_256), SCRAM_SHA_512(ScramMechanism.SCRAM_SHA_512), UNKNOWN(ScramMechanism.UNKNOWN);

  private final ScramMechanism mechanism;

  SaslMechanism(ScramMechanism scramSha256) {
    this.mechanism = scramSha256;
  }

  public String getMechanismName() {
    return mechanism.mechanismName();
  }
}
