package no.mnemonic.messaging.documentchannel.kafka;

import no.mnemonic.commons.utilities.collections.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * This iterator implementation will return a range of documents
 * from kafka, until the iterator reaches the cursor limit (for all partitions)
 *
 * @param <T> type of document to return
 */
public class KafkaRangeIterator<T> implements Iterator<KafkaDocument<T>>, Iterable<KafkaDocument<T>> {

  private final KafkaDocumentSource<T> source;
  private final Map<TopicPartition, KafkaCursor.OffsetAndTimestamp> partitionsWithData = MapUtils.map();
  private final Queue<KafkaDocument<T>> queue = new ArrayDeque<>();
  private final KafkaCursor cursor;

  /**
   * @param source The kafka document source to read from. The source should be seeked to the proper start position.
   * @param fromCursor a {@link KafkaCursor} string representing the start position to iterate from, as returned from a {@link KafkaDocument#getCursor()}. FromCursor MAY be null (will then iterate from the default starting point)
   * @param toCursor a {@link KafkaCursor} string representing the end of the target range, as returned from a {@link KafkaDocument#getCursor()}. ToCursor MUST be set, or will throw IllegalArgumentException.
   */
  public KafkaRangeIterator(KafkaDocumentSource<T> source, String fromCursor, String toCursor) throws KafkaInvalidSeekException, InterruptedException {
    this.source = source;
    if (source == null) throw new IllegalArgumentException("source not set!");
    if (toCursor == null) throw new IllegalArgumentException("toCursor not set!");

    source.seek(fromCursor);
    KafkaCursor parsedCursor = KafkaCursor.valueOf(toCursor);

    //create map of partitions whose data we want to fetch
    for (Map.Entry<String, Map<Integer, KafkaCursor.OffsetAndTimestamp>> topicEntry : parsedCursor.getPointers().entrySet()) {
      topicEntry.getValue().forEach((partition,offsetAndTimestamp)->partitionsWithData.put(
              new TopicPartition(topicEntry.getKey(), partition), offsetAndTimestamp)
      );
    }

    //create a local cursor pointing to the current state of the source
    cursor = new KafkaCursor(source.getKafkaCursor());
  }

  @Override
  public Iterator<KafkaDocument<T>> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    //if more data in local queue, use that first
    if (!queue.isEmpty()) return true;
    //if no more partitions to fetch from, we are done
    if (partitionsWithData.isEmpty()) return false;
    //try fetching more data
    fetchNextBatch();
    //if queue is still empty, we are done
    return !queue.isEmpty();
  }

  @Override
  public KafkaDocument<T> next() {
    if (!hasNext()) throw new NoSuchElementException();
    return queue.poll();
  }

  private void fetchNextBatch() {
    //fetch next batch consumer records from source
    ConsumerRecords<String, T> records = source.pollConsumerRecords(Duration.ofSeconds(1));
    for (ConsumerRecord<String, T> rec : records) {
      TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
      KafkaCursor.OffsetAndTimestamp offsetAndTimestamp = partitionsWithData.get(tp);
      //if the partition this document comes from is done, skip it
      if (offsetAndTimestamp == null || rec.offset() > offsetAndTimestamp.getOffset()) {
        partitionsWithData.remove(tp);
        continue;
      }
      //update cursor
      cursor.register(rec);
      //add to output queue
      queue.add(new KafkaDocument<>(rec, cursor.toString()));
    }
  }

}
