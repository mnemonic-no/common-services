KAFKA Document Source
=====================

This package contains a Kafka implementation of the DocumentChannel framework.

## Usage

A document channel consists of a *producer* side and a *consumer* side. 
Depending on the channel implementation and configuration, there may be multiple producers or consumers.

### Set up a Kafka Destination

```
KafkaProducerProvider provider = KafkaProducerProvider.builder()
            .setKafkaHosts("mykafka")
            .setKafkaPort(9094)
            .build();
            
KafkaDocumentDestination<String> destination = KafkaDocumentDestination.<String>builder()
            .setProducerProvider(provider)
            .setFlushAfterWrite(false)
            .setTopicName(topic)
            .setType(String.class)
            .build();
            
destination.getDocumentChannel().sendDocument("mydoc1");                        
```

### Set up a Kafka Source
```
KafkaConsumerProvider provider = KafkaConsumerProvider.builder()
            .setKafkaHosts("mykafka"))
            .setKafkaPort(90924)
            .setGroupID("ConsumerGroup")
            .build();
                        
KafkaDocumentSource<String> source = KafkaDocumentSource.<String>builder()
            .setProducerProvider(provider)
            .setTopicName(topic)
            .setType(String.class)
            .build();
            
            
DocumentBatch<String> batch = source.poll(Duration.ofSeconds(10));
Collect<String> result = batch.getDocuments();
batch.acknowledge();            
```

### Set up a listener

By setting up a listener, Kafka will push messages to a `DocumentChannelListener` interface.

```
source.createDocumentSubscription(doc->System.out.println("Received " + doc));
```

### Controlling acknowledgement

By default, a Kafka consumer is set up to not automatically commit polling, so after processing a batch, 
the client should acknowledge the batch by invoking `batch.acknowledge()`.
To automatically acknowledge batches, use the `KafkaDocumentSource.Builder.setAutoCommit(true)`.

For listeners, Kafka will automatically acknowledge documents delivered to the listener interface at the end of each batch, 
unless the document source is set up with `KafkaDocumentSource.Builder.setCommitType(none)`.

By default, the commit type is set to `async`. To ensure that commit is propagated to Kafka when calling `acknowledge()`, use
`KafkaDocumentSource.Builder.setCommitType(sync)`.

### Using custom serializers

To use the kafka channel with complex object types, the producer and consumer must be set up with custom serializers.

```
KafkaProducerProvider destProvider = KafkaProducerProvider.builder()
            .setKafkaHosts("mykafka")
            .setKafkaPort(9094)
            .addSerializer(MyType.class, MyTypeSerializer.class)
            .build();
            
KafkaDocumentDestination<MyType> destination = KafkaDocumentDestination.<String>builder()
            .setProducerProvider(destProvider)
            .setFlushAfterWrite(false)
            .setTopicName(topic)
            .setType(MyType.class)
            .build();

KafkaConsumerProvider provider = KafkaConsumerProvider.builder()
            .setKafkaHosts("mykafka"))
            .setKafkaPort(9092)
            .setGroupID("ConsumerGroup")
            .addDeserializer(MyType.class, MyTypeSerializer.class)
            .build();
                        
KafkaDocumentSource<MyType> source = KafkaDocumentSource.<String>builder()
            .setProducerProvider(provider)
            .setTopicName(topic)
            .setType(MyType.class)
            .build();            
```

### Using cursors and seek()

Kafka has a server-side retention of consumed data, even after the client has consumed and acknowledged it. 
This allows a client to reconnect and go back to a previous point and re-consume the data.

To use this, the client should use either 
`DocumentBatch<KafkaDocument<T>> KafkaDocumentSource.pollDocuments(Duration)`
or register a document subscription using the extended interface
```
public interface KafkaDocumentChannelListener<T> extends DocumentChannelListener<T> {
  void documentReceived(KafkaDocument<T> document);
}
```

This will deliver the document to the client wrapped in a `KafkaDocument` object, which contains a *cursor*.
To reconnect and resume consumption at the point of the cursor, a client can

```
String lastCursor = null;
DocumentBatch<KafkaDocument<String>> batch = source.pollDocuments(Duration.ofSeconds(10));
for (KafkaDocument<String> doc : batch) {
  handle(doc.getDocument());
  lastCursor=doc.getCursor();
}

...

//reconnect and replay
KafkaDocumentSource<String> source = KafkaDocumentSource.<String>builder()
            .setProducerProvider(provider)
            .setTopicName(topic)
            .setType(String.class)
            .build();
source.seek(lastCursor);

//continue consumption
DocumentBatch<KafkaDocument<String>> batch = source.pollDocuments(Duration.ofSeconds(10));
...
       
```

### Consuming a limited range

If a client resumes "live" consumption from the latest offset of the topic, the client may want to go back and fetch 
the documents from the channel between the last received cursor from the previous connection, and the first received 
document from the new connection.

To do this, the client can use a separate `KafkaDocumentSource` with a `KafkaRangeIterator`:

```
KafkaConsumerProvider provider = KafkaConsumerProvider.builder()
            .setKafkaHosts("mykafka"))
            .setKafkaPort(90924)
            //use a random groupID to ensure that this consumer group is not distributed
            //as we want to receive ALL messages
            .setGroupID(UUID.randomUUID.toString())
            .build();
KafkaDocumentSource<String> source = KafkaDocumentSource.<String>builder()
            .setProducerProvider(provider)
            .setTopicName(topic)
            .setType(String.class)
            //set commitType none to avoid writing commit for the random groupID to the kafka server
            .setCommitType(none)
            .build();
source.seek(lastCursorFromOldConnection);

Iterator<KafkaDocument<String>> iterator = new KafkaRangeIterator(source, firstCursorFromNewConnection);
for (KafkaDocument<String> doc : iterator) {
    handle(doc.getDocument());
}                        
```