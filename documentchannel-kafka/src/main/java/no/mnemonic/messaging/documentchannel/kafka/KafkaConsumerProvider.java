package no.mnemonic.messaging.documentchannel.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static no.mnemonic.commons.utilities.collections.MapUtils.map;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * A provider which provides a kafka consumer for a kafka cluster and groupID.
 */
public class KafkaConsumerProvider {

  private static final int DEFAULT_REQUEST_TIMEOUT_MILLIS = 30000;
  private static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 15000; // Need > consumer poll timeout, to avoid exception of time between subsequent calls to poll() was longer than the configured session.timeout.ms
  private static final int DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 3000;
  private static final int DEFAULT_MAX_POLL_RECORDS = 100;
  private static final int DEFAULT_MAX_POLL_INTERVAL_MILLIS = (int) TimeUnit.MINUTES.toMillis(5L);

  public enum OffsetResetStrategy {
    latest, earliest, none;
  }

  private final String kafkaHosts;
  private final int kafkaPort;
  private final String groupID;
  private final OffsetResetStrategy offsetResetStrategy;
  private final boolean autoCommit;
  private final int heartbeatIntervalMs;
  private final int requestTimeoutMs;
  private final int sessionTimeoutMs;
  private final int maxPollRecords;
  private final int maxPollIntervalMs;
  private final Map<Class<?>, Deserializer<?>> deserializers;
  private final SaslMechanism saslMechanism;
  private final String saslUsername;
  private final String saslPassword;

  private KafkaConsumerProvider(
          String kafkaHosts,
          int kafkaPort,
          String groupID,
          OffsetResetStrategy offsetResetStrategy,
          boolean autoCommit,
          int heartbeatIntervalMs,
          int requestTimeoutMs,
          int sessionTimeoutMs,
          int maxPollRecords,
          int maxPollIntervalMs,
          Map<Class<?>, Deserializer<?>> deserializers,
          SaslMechanism saslMechanism,
          String saslUsername,
          String saslPassword) {
    this.kafkaHosts = kafkaHosts;
    this.kafkaPort = kafkaPort;
    this.groupID = groupID;
    this.offsetResetStrategy = offsetResetStrategy;
    this.autoCommit = autoCommit;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.maxPollRecords = maxPollRecords;
    this.maxPollIntervalMs = maxPollIntervalMs;
    this.deserializers = deserializers;
    this.saslMechanism = saslMechanism;
    this.saslUsername = saslUsername;
    this.saslPassword = saslPassword;

    this.deserializers.put(String.class, new StringDeserializer());
    this.deserializers.put(byte[].class, new ByteArrayDeserializer());

  }

  public <T> KafkaConsumer<String, T> createConsumer(Class<T> type) {
    return new KafkaConsumer<>(
            createProperties(),
            new StringDeserializer(),
            getDeserializer(type)
    );
  }

  public boolean hasType(Class<?> type) {
    return deserializers.containsKey(type);
  }

  private <T> Deserializer<T> getDeserializer(Class<T> type) {
    if (!hasType(type))
      throw new IllegalArgumentException("Invalid type: " + type);
    //noinspection unchecked
    return (Deserializer<T>) deserializers.get(type);

  }

  private Map<String, Object> createProperties() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(BOOTSTRAP_SERVERS_CONFIG, createBootstrapServerList()); // expect List<String>
    properties.put(GROUP_ID_CONFIG, groupID);
    properties.put(AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy.name());
    properties.put(ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
    properties.put(CLIENT_ID_CONFIG, UUID.randomUUID().toString());

    properties.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
    properties.put(SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
    properties.put(MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    properties.put(MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    properties.put(HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
    if (saslMechanism != SaslMechanism.NONE) {
      // https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_scram.html#clients
      properties.put(SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
      properties.put(SaslConfigs.SASL_MECHANISM, saslMechanism.getMechanismName());
      properties.put(SaslConfigs.SASL_JAAS_CONFIG,
              String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                      saslUsername, saslPassword));
    }
    return properties;
  }

  protected List<String> createBootstrapServerList() {
    return Arrays.stream(kafkaHosts.split(","))
            .map(h -> String.format("%s:%d", h, kafkaPort))
            .collect(Collectors.toList());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String kafkaHosts;
    private int kafkaPort;
    private String groupID;

    private OffsetResetStrategy offsetResetStrategy = OffsetResetStrategy.earliest;
    private boolean autoCommit = false;
    private int heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MILLIS;
    private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MILLIS;
    private int sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MILLIS;
    private int maxPollRecords = DEFAULT_MAX_POLL_RECORDS;
    private int maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL_MILLIS;
    private Map<Class<?>, Deserializer<?>> deserializers = map();
    private SaslMechanism saslMechanism = SaslMechanism.NONE;
    private String saslUsername;
    private String saslPassword;

    public KafkaConsumerProvider build() {
      return new KafkaConsumerProvider(kafkaHosts, kafkaPort, groupID, offsetResetStrategy, autoCommit, heartbeatIntervalMs,
              requestTimeoutMs, sessionTimeoutMs, maxPollRecords, maxPollIntervalMs, deserializers, saslMechanism,
              saslUsername, saslPassword);
    }

    public Builder setKafkaHosts(String kafkaHosts) {
      this.kafkaHosts = kafkaHosts;
      return this;
    }

    public Builder setKafkaPort(int kafkaPort) {
      this.kafkaPort = kafkaPort;
      return this;
    }

    public Builder setGroupID(String groupID) {
      this.groupID = groupID;
      return this;
    }

    public Builder setOffsetResetStrategy(OffsetResetStrategy offsetResetStrategy) {
      this.offsetResetStrategy = offsetResetStrategy;
      return this;
    }

    public Builder setAutoCommit(boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    public Builder setHeartbeatIntervalMs(int heartbeatIntervalMs) {
      this.heartbeatIntervalMs = heartbeatIntervalMs;
      return this;
    }

    public Builder setRequestTimeoutMs(int requestTimeoutMs) {
      this.requestTimeoutMs = requestTimeoutMs;
      return this;
    }

    public Builder setSessionTimeoutMs(int sessionTimeoutMs) {
      this.sessionTimeoutMs = sessionTimeoutMs;
      return this;
    }

    public Builder setMaxPollRecords(int maxPollRecords) {
      this.maxPollRecords = maxPollRecords;
      return this;
    }

    public Builder setMaxPollIntervalMs(int maxPollIntervalMs) {
      this.maxPollIntervalMs = maxPollIntervalMs;
      return this;
    }

    public <T> Builder addDeserializer(Class<T> type, Deserializer<T> deserializer) {
      if (type == null) throw new IllegalArgumentException("type not set");
      if (deserializer == null) throw new IllegalArgumentException("deserializer not set");
      this.deserializers.put(type, deserializer);
      return this;
    }

    public Builder setSaslMechanism(SaslMechanism saslMechanism) {
      this.saslMechanism = saslMechanism;
      return this;
    }

    public Builder setSaslUsername(String saslUsername) {
      this.saslUsername = saslUsername;
      return this;
    }

    public Builder setSaslPassword(String saslPassword) {
      this.saslPassword = saslPassword;
      return this;
    }
  }
}
