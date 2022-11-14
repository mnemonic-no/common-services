# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.44] - 2022.11.15
### Added
ARGUS-32084
- Exposing `KafkaToHazelcastHandler.getQueueSize()`

## [0.3.43] - 2022.11.07
### Fixed
ARGUS-31871
- Properly shut down worker threads on InterruptedException in Hazelcast consumer pipeline.

## [0.3.42] - 2022.11.02
### Changed
ARGUS-31809
- Made termination timeout configurable in `HazelcastTransactionalConsumerHandler`

## [0.3.41] - 2022.10.16
### Added
ARGUS-31391
- Add new package `hazelcast5-consumer` which uses Hazelcast 4 or 5

### Upgrade notes
* Replace package `hazelcast-consumer` with `hazelcast-consumer5`
* Update other client code to use Hazelcast 4 or 5

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

