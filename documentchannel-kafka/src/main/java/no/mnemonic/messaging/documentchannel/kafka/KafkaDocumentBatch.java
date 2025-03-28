package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.messaging.documentchannel.DocumentBatch;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Set;

public class KafkaDocumentBatch<D> implements DocumentBatch<D> {

  private static final Logger LOGGER = Logging.getLogger(KafkaDocumentBatch.class);

  private final Collection<KafkaDocument<D>> documents;
  private final KafkaDocumentSource.CommitType commitType;
  private final KafkaConsumer<?, ?> consumer;

  KafkaDocumentBatch(Collection<KafkaDocument<D>> documents, KafkaDocumentSource.CommitType commitType, KafkaConsumer<?, ?> consumer) {
    this.documents = documents;
    this.commitType = commitType;
    this.consumer = consumer;
  }

  @Override
  public Collection<D> getDocuments() {
    return ListUtils.list(documents, KafkaDocument::getDocument);
  }

  public Collection<KafkaDocument<D>> getKafkaDocuments() {
    return documents;
  }

  @Override
  public void acknowledge() {
    if (commitType == KafkaDocumentSource.CommitType.sync) {
      consumer.commitSync();
    } else if (commitType == KafkaDocumentSource.CommitType.async) {
      consumer.commitAsync();
    }
  }

  @Override
  public void reject() {
    resetOffsets();
  }

  private void resetOffsets() {
    LOGGER.info("Reset offsets for assigned Kafka partitions " + consumer.assignment());
    Set<TopicPartition> assignment = consumer.assignment();
    consumer.committed(assignment)
            .entrySet().stream()
            // no offsets can be fetched from Kafka broker (e.g. no commits has been performed on the partition)
            .filter(e -> e.getValue() != null)
            .forEach(e -> consumer.seek(e.getKey(), e.getValue()));
  }
}
