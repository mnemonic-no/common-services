package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.ListUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaTopicHelper implements AutoCloseable {

  private static final Logger LOGGER = Logging.getLogger(KafkaTopicHelper.class);
  private final Admin admin;

  public KafkaTopicHelper(String bootstrapServers) {
    this(ListUtils.list(bootstrapServers));
  }

  public KafkaTopicHelper(List<String> bootstrapServers) {
    Properties properties = new Properties();
    properties.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
    );
    admin = Admin.create(properties);
  }

  @Override
  public void close() {
    admin.close();
  }

  public Set<String> listTopics() throws ExecutionException, InterruptedException {
    return admin.listTopics().listings().get()
            .stream()
            .map(TopicListing::name)
            .collect(Collectors.toSet());
  }

  public void createMissingTopic(String topicName) throws ExecutionException, InterruptedException {
    createMissingTopic(topicName, 1, (short)1);
  }

  public void createMissingTopic(String topicName, int partitions, short replicationFactor) throws ExecutionException, InterruptedException {
    if (listTopics().contains(topicName)) {
      LOGGER.info("Topic %s already exists", topicName);
      return;
    };

    LOGGER.info("Creating missing topic %s", topicName);
    NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

    CreateTopicsResult result = admin.createTopics(
            Collections.singleton(newTopic)
    );

    KafkaFuture<Void> future = result.values().get(topicName);
    future.get();
  }
}
