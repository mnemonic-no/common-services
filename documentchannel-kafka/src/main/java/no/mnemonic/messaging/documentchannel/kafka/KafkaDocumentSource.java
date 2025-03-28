package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.logging.Logger;
import no.mnemonic.commons.logging.Logging;
import no.mnemonic.commons.metrics.MetricAspect;
import no.mnemonic.commons.metrics.MetricException;
import no.mnemonic.commons.metrics.Metrics;
import no.mnemonic.commons.metrics.MetricsData;
import no.mnemonic.commons.utilities.collections.CollectionUtils;
import no.mnemonic.commons.utilities.collections.ListUtils;
import no.mnemonic.commons.utilities.collections.MapUtils;
import no.mnemonic.commons.utilities.lambda.LambdaUtils;
import no.mnemonic.messaging.documentchannel.DocumentChannelListener;
import no.mnemonic.messaging.documentchannel.DocumentChannelSubscription;
import no.mnemonic.messaging.documentchannel.DocumentSource;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static no.mnemonic.commons.utilities.ObjectUtils.ifNotNullDo;
import static no.mnemonic.commons.utilities.collections.ListUtils.addToList;
import static no.mnemonic.commons.utilities.collections.ListUtils.list;
import static no.mnemonic.commons.utilities.collections.MapUtils.map;
import static no.mnemonic.commons.utilities.collections.SetUtils.addToSet;
import static no.mnemonic.commons.utilities.collections.SetUtils.set;
import static no.mnemonic.commons.utilities.lambda.LambdaUtils.tryTo;

/**
 * Kafka version of a document channel source. The source is configured with a kafka cluster, topic and groupID.
 * Multiple sources may be configured to the same topic and groupID, which will load-balance the incoming documents
 * between the active sources.
 * <p>
 * Multiple sources with different groupIDs will receive individual copies of each document on the topic.
 *
 * @param <T> document type
 */
public class KafkaDocumentSource<T> implements DocumentSource<T>, MetricAspect {

  private static final Logger LOGGER = Logging.getLogger(KafkaDocumentSource.class);
  private static final long DEFAULT_MAX_ASSIGNMENT_WAIT_MS = TimeUnit.SECONDS.toMillis(10);

  public enum CommitType {
    sync, async, none;
  }

  private final KafkaConsumerProvider provider;
  private final Class<T> type;
  private final List<String> topicName;
  private final CommitType commitType;

  private final AtomicBoolean subscriberAttached = new AtomicBoolean();
  private final AtomicReference<KafkaConsumer<String, T>> currentConsumer = new AtomicReference<>();
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final Set<Consumer<Exception>> errorListeners;

  private final AtomicBoolean consumerRunning = new AtomicBoolean();
  private final LongAdder consumerRetryProcessError = new LongAdder();
  private final LongAdder consumerRetryKafkaError = new LongAdder();
  private final KafkaCursor currentCursor = new KafkaCursor();

  private final CountDownLatch assignmentLatch = new CountDownLatch(1);

  private final long maxAssignmentWait;

  private KafkaDocumentSource(
          KafkaConsumerProvider provider,
          Class<T> type,
          List<String> topicName,
          CommitType commitType,
          Set<Consumer<Exception>> errorListeners,
          boolean createIfMissing,
          long maxAssignmentWait
  ) {
    this.commitType = commitType;
    this.errorListeners = errorListeners;
    this.maxAssignmentWait = maxAssignmentWait;
    if (provider == null) throw new IllegalArgumentException("provider not set");
    if (type == null) throw new IllegalArgumentException("type not set");
    if (CollectionUtils.isEmpty(topicName)) throw new IllegalArgumentException("topicName not set");
    if (!provider.hasType(type))
      throw new IllegalArgumentException("Provider does not support type, maybe add a deserializer for " + type);
    this.provider = provider;
    this.type = type;
    this.topicName = topicName;

    if (createIfMissing) {
      LOGGER.info("Creating any missing topics in %s", topicName);
      try (KafkaTopicHelper helper = new KafkaTopicHelper(provider.createBootstrapServerList())) {
        for (String t : topicName) {
          LambdaUtils.tryTo(() -> helper.createMissingTopic(t), ex -> LOGGER.error(ex, "Error creating topic %s", t));
        }
      }
    }
  }

  /**
   * Seek to cursor
   *
   * @param cursor A string representation of a cursor as returned in a {@link KafkaDocument}. A null cursor will not change the offset of the consumer.
   * @throws KafkaInvalidSeekException if the cursor is invalid, or points to a timestamp which is not reachable
   */
  public void seek(String cursor) throws KafkaInvalidSeekException {
    //for each active topic, seek each partition to the position specified by the cursor
    if (currentConsumer.get() != null) {
      throw new IllegalStateException("Cannot seek for consumer which is already set");
    }

    currentConsumer.set(provider.createConsumer(type));

    assign(set(topicName));
    if (cursor != null) {
      //parse cursor into current cursor
      currentCursor.parse(cursor);
      for (String topic : topicName) {
        seek(topic, currentCursor.getPointers().get(topic));
      }
    }
  }

  @Override
  public Metrics getMetrics() throws MetricException {
    MetricsData metrics = new MetricsData()
            .addData("processing.error.retry", consumerRetryProcessError.longValue())
            .addData("kafka.error.retry", consumerRetryKafkaError.longValue());

    //only report alive-metric if this source has ever been attached to a subscriber
    if (subscriberAttached.get()) {
      metrics.addData("alive", consumerRunning.get() ? 1 : 0);
    }
    return metrics;
  }

  /**
   * Let client wait for partition assignment to complete. Method is reentrant,
   * so calling this AFTER assignment is already done will immediately return true.
   *
   * @param maxWait max duration to wait before returning false
   * @return true if partitions are assigned, or false if no partitions were assigned before the timeout
   */
  public boolean waitForAssignment(Duration maxWait) throws InterruptedException {
    return assignmentLatch.await(maxWait.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Fetch the current cursor of this source.
   * The method will block up to <code>maxAssignmentWait</code> milliseconds waiting for partitions assignment.
   * <p>
   * Note; this cursor is not thread safe; if there is a separate consumer thread polling, you should rather
   * use the cursor of the KafkaDocument.
   *
   * @return the String cursor for the current position of this source
   * @throws InterruptedException if interrupted while waiting for Kafka to assign partitions
   * @see KafkaDocument
   */
  public String getCursor() throws InterruptedException {
    return getKafkaCursor().toString();
  }

  KafkaCursor getKafkaCursor() throws InterruptedException {
    if (!waitForAssignment(Duration.ofMillis(maxAssignmentWait))) {
      throw new IllegalStateException(String.format("No assignment in %d ms for topics %s", maxAssignmentWait, topicName));
    }
    return currentCursor;
  }

  @Override
  public DocumentChannelSubscription createDocumentSubscription(DocumentChannelListener<T> listener) {
    if (listener == null) throw new IllegalArgumentException("listener not set");
    if (subscriberAttached.get()) throw new IllegalStateException("subscriber already added");
    KafkaConsumerWorker<T> worker = new KafkaConsumerWorker<>(this, listener, createCallbackInterface());
    executorService.submit(worker);
    subscriberAttached.set(true);

    return worker::cancel;
  }

  private ConsumerCallbackInterface createCallbackInterface() {
    return new ConsumerCallbackInterface() {
      @Override
      public void consumerRunning(boolean running) {
        consumerRunning.set(running);
      }

      @Override
      public void retryError(Exception e) {
        consumerRetryKafkaError.increment();
        errorListeners.forEach(l -> l.accept(e));
      }

      @Override
      public void register(ConsumerRecord<?, ?> record) {
        currentCursor.register(record);
      }

      @Override
      public void processError(Exception e) {
        consumerRetryProcessError.increment();
        errorListeners.forEach(l -> l.accept(e));
      }

      @Override
      public boolean isShutdown() {
        return executorService.isShutdown();
      }

      @Override
      public String cursor() {
        return currentCursor.toString();
      }
    };
  }

  @Override
  public KafkaDocumentBatch<T> poll(Duration duration) {
    return createBatch(pollConsumerRecords(duration));
  }

  @Override
  public void close() {
    executorService.shutdown();
    tryTo(() -> executorService.awaitTermination(10, TimeUnit.SECONDS));
    currentConsumer.updateAndGet(c -> {
      ifNotNullDo(c, KafkaConsumer::close);
      return null;
    });
  }

  public KafkaConsumer<String, T> getKafkaConsumer() {
    return getCurrentConsumerOrSubscribe();
  }

  //private methods

  ConsumerRecords<String, T> pollConsumerRecords(Duration duration) {
    if (duration == null) throw new IllegalArgumentException("duration not set");
    //the semantics of the document source does not require an explicit "subscribe()"-operation
    return getCurrentConsumerOrSubscribe().poll(duration);
  }

  private void assign(Set<String> topics) {
    //determine active partitions for this topic
    Set<TopicPartition> partitionInfos = set(topics).stream()
            .map(topic -> set(getConsumerOrFail().partitionsFor(topic)))
            .flatMap(Collection::stream)
            .map(p -> new TopicPartition(p.topic(), p.partition()))
            .collect(Collectors.toSet());

    //assign all partitions to this consumer
    getConsumerOrFail().assign(partitionInfos);
    //update the cursor to point to this position
    currentCursor.set(getConsumerOrFail());
    //free latch
    assignmentLatch.countDown();
  }

  private void seek(String topic, Map<Integer, KafkaCursor.OffsetAndTimestamp> cursorPointers) throws KafkaInvalidSeekException {
    if (MapUtils.isEmpty(cursorPointers)) return;

    Set<Integer> partitions = getConsumerOrFail().partitionsFor(topic).stream().map(p -> p.partition()).collect(Collectors.toSet());
    for (Integer partition : cursorPointers.keySet()) {
      if (!partitions.contains(partition)) {
        throw new KafkaInvalidSeekException("Unable to seek invalid partition " + partition + " for topic " + topic);
      }
    }

    //If the cursor contains timestamp: find timestamp to seek to for each partition in the cursor (all other partitions will stay at initial offset set by OffsetResetStrategy)
    Map<TopicPartition, Long> partitionsToSeek = new HashMap<>();
    map(cursorPointers).entrySet().stream().filter(cp -> cp.getValue().getTimestamp() > 0).forEach(partitionTimestamp -> partitionsToSeek.put(
            new TopicPartition(topic, partitionTimestamp.getKey()),
            partitionTimestamp.getValue().getTimestamp()
    ));
    //lookup offsets by timestamp from Kafka
    Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = getConsumerOrFail().offsetsForTimes(partitionsToSeek);
    //for each partition we have timestamp for, seek to requested position, or fail
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entries : partitionOffsetMap.entrySet()) {
      if (entries.getValue() == null)
        throw new KafkaInvalidSeekException("Unable to skip to requested position for partition " + entries.getKey().partition());
      getConsumerOrFail().seek(entries.getKey(), entries.getValue().offset());
    }

    //for each partition we have offset (but no timestamp) for, seek to requested position, or fail
    List<Map.Entry<Integer, KafkaCursor.OffsetAndTimestamp>> partitionOffsetsToSeekTo = cursorPointers.entrySet().stream()
            .filter(cp -> cp.getValue().getTimestamp() == 0)
            .collect(Collectors.toList());
    for (Map.Entry<Integer, KafkaCursor.OffsetAndTimestamp> partition : partitionOffsetsToSeekTo) {
      getConsumerOrFail().seek(new TopicPartition(topic, partition.getKey()), partition.getValue().getOffset());
    }
    //now we are assigned, so free the latch
    assignmentLatch.countDown();
  }

  private KafkaDocumentBatch<T> createBatch(ConsumerRecords<String, T> records) {
    List<KafkaDocument<T>> result = ListUtils.list();
    if (records != null) {
      for (ConsumerRecord<String, T> rec : records) {
        currentCursor.register(rec);
        result.add(new KafkaDocument<>(rec, currentCursor.toString()));
      }
    }
    return new KafkaDocumentBatch<>(result, commitType, getConsumerOrFail());
  }

  private KafkaConsumer<String, T> getConsumerOrFail() {
    if (currentConsumer.get() == null) throw new IllegalStateException("Consumer not set");
    return currentConsumer.get();
  }

  private KafkaConsumer<String, T> getCurrentConsumerOrSubscribe() {
    currentConsumer.get();
    if (currentConsumer.get() == null) {
      KafkaConsumer<String, T> consumer = provider.createConsumer(type);
      consumer.subscribe(topicName, new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          LOGGER.info("Partitions were revoked: " + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          LOGGER.info("Partitions were assigned: " + partitions);
          //set the position of the cursor to the current position of the consumer
          currentCursor.set(consumer);
          //release the latch
          assignmentLatch.countDown();
        }
      });
      currentConsumer.set(consumer);
    }
    return currentConsumer.get();
  }

  public interface ConsumerCallbackInterface {
    void consumerRunning(boolean running);

    void retryError(Exception e);

    void register(ConsumerRecord<?, ?> record);

    void processError(Exception e);

    boolean isShutdown();

    String cursor();
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    //fields
    private KafkaConsumerProvider kafkaConsumerProvider;
    private Class<T> type;
    private List<String> topicName;
    private CommitType commitType = CommitType.async;
    private Set<Consumer<Exception>> errorListeners = set();
    private boolean createIfMissing;
    private long maxAssignmentWait = DEFAULT_MAX_ASSIGNMENT_WAIT_MS;

    public KafkaDocumentSource<T> build() {
      return new KafkaDocumentSource<>(kafkaConsumerProvider, type, topicName, commitType, errorListeners, createIfMissing, maxAssignmentWait);
    }

    //setters

    /**
     * This parameter only applies to getCursor(), and should probably never need to be changed.
     *
     * @param maxAssignmentWait max millis to wait for assignments.
     */
    public Builder<T> setMaxAssignmentWait(long maxAssignmentWait) {
      this.maxAssignmentWait = maxAssignmentWait;
      return this;
    }

    public Builder<T> setErrorListeners(Set<Consumer<Exception>> errorListeners) {
      this.errorListeners = set(errorListeners);
      return this;
    }

    public Builder addErrorListener(Consumer<Exception> errorListener) {
      this.errorListeners = addToSet(this.errorListeners, errorListener);
      return this;
    }

    public Builder<T> setCommitSync(boolean sync) {
      return setCommitType(sync ? CommitType.sync : CommitType.async);
    }

    public Builder<T> setCommitType(CommitType commitType) {
      this.commitType = commitType;
      return this;
    }

    public Builder<T> setConsumerProvider(KafkaConsumerProvider kafkaConsumerProvider) {
      this.kafkaConsumerProvider = kafkaConsumerProvider;
      return this;
    }

    public Builder<T> setType(Class<T> type) {
      this.type = type;
      return this;
    }

    public Builder<T> setTopicName(String... topicName) {
      this.topicName = list(topicName);
      return this;
    }

    public Builder<T> setTopicName(List<String> topicName) {
      this.topicName = topicName;
      return this;
    }

    public Builder<T> addTopicName(String topicName) {
      this.topicName = addToList(this.topicName, topicName);
      return this;
    }

    public Builder<T> setCreateIfMissing(boolean createIfMissing) {
      this.createIfMissing = createIfMissing;
      return this;
    }
  }

}
