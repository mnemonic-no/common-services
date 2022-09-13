# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.38] - 2022-09-13
### Changed
ARGUS-29853
- use DocumentSource instead of KafkaDocumentSource in KafkaToHazelcastHandler constructor.
## [0.3.37] - 2022-08-29
### Changed
ARGUS-30461
- `KafkaToHazelcastHandler` is now using its own internal `KafkaWorker` thread to poll batches of documents.
  This allows the handler to roll back the entire batch if writing to Hazelcast fails.
- Added new metric `bulk.failed.count` to the `HazelcastTransactionalConsumer`.

