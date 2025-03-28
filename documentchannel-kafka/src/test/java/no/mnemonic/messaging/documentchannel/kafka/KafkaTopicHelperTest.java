package no.mnemonic.messaging.documentchannel.kafka;

import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import no.mnemonic.commons.jupiter.docker.DockerTestUtils;
import no.mnemonic.commons.utilities.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class KafkaTopicHelperTest {

  static {
    if (StringUtils.isBlank(System.getenv().get("DOCKER_HOSTNAME"))) {
      throw new IllegalStateException("Missing mandatory environment variable DOCKER_HOSTNAME");
    }
  }

  @RegisterExtension
  public static DockerComposeExtension docker = DockerComposeExtension.builder()
          .file("src/test/resources/docker-compose-kafka.yml")
          .projectName(ProjectName.fromString(UUID.randomUUID().toString().replace("-", "")))
          .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
          .build();

  @Test
  public void createTopic() throws InterruptedException, ExecutionException {
    String topicName = UUID.randomUUID().toString();

    try (KafkaTopicHelper kafkaTopicHelper = new KafkaTopicHelper(String.format("%s:%d", kafkaHost(), kafkaPort()))) {
      kafkaTopicHelper.createMissingTopic(topicName);
      assertTrue(kafkaTopicHelper.listTopics().contains(topicName));
    }
  }

  @Test
  public void reCreateTopicIsIgnored() throws InterruptedException, ExecutionException {
    String topicName = UUID.randomUUID().toString();

    try (KafkaTopicHelper kafkaTopicHelper = new KafkaTopicHelper(String.format("%s:%d", kafkaHost(), kafkaPort()))) {
      kafkaTopicHelper.createMissingTopic(topicName);
      kafkaTopicHelper.createMissingTopic(topicName);
      assertTrue(kafkaTopicHelper.listTopics().contains(topicName));
    }
  }

  private String kafkaHost() {
    return DockerTestUtils.getDockerHost();
  }

  private int kafkaPort() {
    return docker.containers()
            .container("kafka")
            .port(9094)
            .getExternalPort();
  }

}