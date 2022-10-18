# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.40] - 2022-10-18
### Changed
ARGUS-31387
- Improved error handling in KafkaToHazelcastHandler and HazelcastTransactionalConsumer.
- Changed default value for `keepThreadAliveOnException` to `true`.
- Changed `bulk.failed.count` metric to only report if a configurable error threshold is exceeded
(default threshold is 3, change with `permittedConsecutiveErrors` option).
- Changed `queue.offer.error.count` metric to `queue.offer.timeout.count`.
- Added additional metrics `bulk.accepted.count` and `bulk.rejected.count`.

## [0.3.39] - 2022-10-06
### Changed
ARGUS-31292
- Add improved exception handling to `KafkaToHazelcastHandler.KafkaWorker`

## [0.3.38] - 2022-09-13
### Changed
ARGUS-29853
- use DocumentSource instead of `KafkaDocumentSource` in `KafkaToHazelcastHandler` constructor.

## [0.3.37] - 2022-08-29
### Changed
ARGUS-30461
- `KafkaToHazelcastHandler` is now using its own internal `KafkaWorker` thread to poll batches of documents.
  This allows the handler to roll back the entire batch if writing to Hazelcast fails.
- Added new metric `bulk.failed.count` to the `HazelcastTransactionalConsumer`.

