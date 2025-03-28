package no.mnemonic.messaging.documentchannel.kafka;

import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import lombok.CustomLog;
import no.mnemonic.commons.jupiter.docker.DockerTestUtils;
import no.mnemonic.commons.utilities.StringUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.documentchannel.DocumentChannel;
import no.mnemonic.messaging.documentchannel.DocumentDestination;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

@CustomLog
@ExtendWith(MockitoExtension.class)
public class KafkaRangeIteratorTest {

  static {
    if (StringUtils.isBlank(System.getenv().get("DOCKER_HOSTNAME"))) {
      throw new IllegalStateException("Missing mandatory environment variable DOCKER_HOSTNAME");
    }
  }

  @Mock
  private Consumer<Exception> errorListener;

  private final Collection<AutoCloseable> channels = new ArrayList<>();
  private String topic1 = UUID.randomUUID().toString();
  private String topic2 = UUID.randomUUID().toString();

  @RegisterExtension
  public static DockerComposeExtension docker = DockerComposeExtension.builder()
          .file("src/test/resources/docker-compose-kafka.yml")
          .projectName(ProjectName.fromString(UUID.randomUUID().toString().replace("-", "")))
          .waitingForService("kafka", HealthChecks.toHaveAllPortsOpen())
          .build();

  @AfterEach
  public void tearDown() {
    channels.forEach(c -> tryTo(c::close));
  }

  @Test
  public void iterateRange() throws KafkaInvalidSeekException, InterruptedException {
    DocumentDestination<String> destination = setupDestination(topic1);
    DocumentChannel<String> documentChannel = destination.getDocumentChannel();

    for (int i = 0; i < 100; i++) {
      documentChannel.sendDocument("doc" + i);
    }
    documentChannel.flush();

    KafkaDocumentSource<String> source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    KafkaDocumentBatch<String> batch = source.poll(Duration.ofSeconds(10));
    List<KafkaDocument<String>> documents = list(batch.getKafkaDocuments());

    KafkaDocument<String> firstDocument = documents.get(0);
    KafkaDocument<String> lastDocument = documents.get(documents.size()-1);
    assertEquals("doc0", firstDocument.getDocument());
    List<String> allDocuments = ListUtils.list(documents, KafkaDocument::getDocument);
    source.close();

    source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    KafkaRangeIterator<String> iterator = new KafkaRangeIterator<>(source, firstDocument.getCursor(), lastDocument.getCursor());
    List<String> replayedDocuments = ListUtils.list(iterator, KafkaDocument::getDocument);

    assertEquals(allDocuments, replayedDocuments);
  }

  @Test
  public void iterateRangeFromMultipleTopics() throws KafkaInvalidSeekException, InterruptedException {
    DocumentDestination<String> destination1 = setupDestination(topic1);
    DocumentDestination<String> destination2 = setupDestination(topic2);
    DocumentChannel<String> documentChannel1 = destination1.getDocumentChannel();
    DocumentChannel<String> documentChannel2 = destination2.getDocumentChannel();

    documentChannel1.sendDocument("initial1");
    documentChannel2.sendDocument("initial2");
    Set<String> allDocuments = set();
    allDocuments.addAll(set("initial1", "initial2"));

    KafkaDocumentSource<String> source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    source.seek(null);
    String fromCursor = source.getCursor();

    for (int i = 0; i < 100; i++) {
      documentChannel1.sendDocument("doc1-" + i);
      documentChannel2.sendDocument("doc2-" + i);
      allDocuments.addAll(set("doc1-" + i, "doc2-" + i));
    }
    documentChannel1.flush();
    documentChannel2.flush();

    source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    source.seek(null);
    String toCursor = source.getCursor();

    source = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
    KafkaRangeIterator<String> iterator = new KafkaRangeIterator<>(source, fromCursor, toCursor);
    List<String> replayedDocuments = ListUtils.list(iterator, KafkaDocument::getDocument);
    assertEquals(allDocuments, set(replayedDocuments));
    assertEquals(202, replayedDocuments.size());
  }

  @Test
  public void rangeIterateWithIterator() throws KafkaInvalidSeekException, InterruptedException {
    useTopic("rangeIterateWithIteratorTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination(topic1);

    for (int i = 0; i<200; i++) {
      senderChannel.getDocumentChannel().sendDocument("mydoc" + i);
      Thread.sleep(1);
    }
    senderChannel.getDocumentChannel().flush();

    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", null, b->b.setMaxPollRecords(50));
    List<KafkaDocument<String>> batch1 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals("mydoc49", batch1.get(49).getDocument());

    List<KafkaDocument<String>> batch2 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals("mydoc99", batch2.get(49).getDocument());

    List<KafkaDocument<String>> batch3 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals("mydoc149", batch3.get(49).getDocument());

    KafkaDocumentSource<String> receiverChannel2 = setupSource("group", null, b->b.setMaxPollRecords(50));

    Iterator<KafkaDocument<String>> rangeIterator = new KafkaRangeIterator<>(receiverChannel2, batch1.get(49).getCursor(), batch2.get(49).getCursor());
    KafkaDocument<String> last = rangeIterator.next();
    assertEquals("mydoc49", last.getDocument());
    while (rangeIterator.hasNext()) {
      last = rangeIterator.next();
    }
    assertEquals("mydoc99", last.getDocument());
  }

  @Test
  public void noCommitWithRangeIterator() throws InterruptedException, KafkaInvalidSeekException {
    useTopic("noCommitWithRangeIteratorTest");
    KafkaDocumentDestination<String> senderChannel = setupDestination(topic1);

    for (int i = 0; i<100; i++) {
      senderChannel.getDocumentChannel().sendDocument("mydoc" + i);
      Thread.sleep(1);
    }
    senderChannel.getDocumentChannel().flush();

    //read all documents to find last cursor
    KafkaDocumentSource<String> receiverChannel1 = setupSource("group", s->s.setCommitType(KafkaDocumentSource.CommitType.none), b->b.setMaxPollRecords(500));
    List<KafkaDocument<String>> batch1 = ListUtils.list(receiverChannel1.poll(Duration.ofSeconds(10)).getKafkaDocuments());
    assertEquals(100, batch1.size());
    String endCursor = batch1.get(99).getCursor();
    receiverChannel1.close();

    //use range iterator to iterate all documents
    {
      KafkaDocumentSource<String> receiverChannel2 = setupSource("group2", s->s.setCommitType(KafkaDocumentSource.CommitType.none), null);
      Iterator<KafkaDocument<String>> iter2 = new KafkaRangeIterator<>(receiverChannel2, null, endCursor);
      KafkaDocument<String> last = iter2.next();
      assertEquals("mydoc0", last.getDocument());
      while (iter2.hasNext()) last = iter2.next();
      assertEquals("mydoc99", last.getDocument());
      receiverChannel2.close();
    }
    //setup new iterator with same group, verify that it still reads from the start (no commit has occurred)
    {
      KafkaDocumentSource<String> receiverChannel2 = setupSource("group");
      Iterator<KafkaDocument<String>> iter2 = new KafkaRangeIterator<>(receiverChannel2, null, endCursor);
      assertEquals("mydoc0", iter2.next().getDocument());
    }
  }



  private KafkaDocumentDestination<String> setupDestination(String topic) {
    KafkaDocumentDestination<String> channel = KafkaDocumentDestination.<String>builder()
            .setProducerProvider(createProducerProvider())
            .setFlushAfterWrite(false)
            .setCreateIfMissing(true)
            .setTopicName(topic)
            .setType(String.class)
            .build();
    channels.add(channel);
    return channel;
  }

  private KafkaDocumentSource<String> setupSource(String group) {
    return setupSource(group, null, null);
  }

  private KafkaDocumentSource<String> setupSource(String group, Consumer<KafkaDocumentSource.Builder<?>> sourceEdit, Consumer<KafkaConsumerProvider.Builder> providerEdit) {
    // noinspection unchecked
    KafkaDocumentSource.Builder<String> builder = KafkaDocumentSource.<String>builder()
            .setConsumerProvider(createConsumerProvider(group, providerEdit))
            .setCreateIfMissing(true)
            .addErrorListener(errorListener)
            .setTopicName(topic1, topic2)
            .setType(String.class)
            .setCommitType(KafkaDocumentSource.CommitType.sync);
    ifNotNullDo(sourceEdit, s->s.accept(builder));
    KafkaDocumentSource<String> channel = builder.build();
    channels.add(channel);
    return channel;
  }

  private KafkaProducerProvider createProducerProvider() {
    return KafkaProducerProvider.builder()
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .build();
  }

  private KafkaConsumerProvider createConsumerProvider(String group, Consumer<KafkaConsumerProvider.Builder> edits) {
    KafkaConsumerProvider.Builder builder = KafkaConsumerProvider.builder()
            .setMaxPollRecords(2)
            .setKafkaHosts(kafkaHost())
            .setKafkaPort(kafkaPort())
            .setAutoCommit(false)
            .setHeartbeatIntervalMs(1000)
            .setRequestTimeoutMs(11000)
            .setSessionTimeoutMs(10000)
            .setMaxPollIntervalMs(2000)
            .setGroupID(group);
    ifNotNullDo(edits, e->e.accept(builder));
    return builder.build();
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

  private void useTopic(String topicPrefix) {
    topic1 = topicPrefix + "1";
    topic2 = topicPrefix + "2";
  }
}