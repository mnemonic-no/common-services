# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.6.16] - 2024-12-09
### Fix
ARGUS-48286
- Handle iteration error when writing streaming response in `ServiceInvocationHandler`.
- Avoid error message `com.fasterxml.jackson.core.JsonGenerationException: Current context not Object but Array` when iteration fails.

## [0.6.15] - 2024-10-25
### Fix
ARGUS-47757
- Fix backwards incompatible change in `ServiceResponseMessage`, when metadata is not requested (nor returned); 
  a pre-0.6.12 client `ResultSetParser` will fail to deserialize the message because the `metaData` key is unknown.
- This fix omits the metaData from the JSON document if it is not set.

## [0.6.13] - 2024-10-25
### Fix
ARGUS-46660
- Added missing method `ServiceProxyMetaDataContext.isSet()`

## [0.6.12] - 2024-10-20
### Added
ARGUS-46660
- Added `ServiceProxyMetaDataContext` to allow service process to register metadata to be added to the resultset sent back to the client
- Added `ServiceClientMetaDataHandler` interface allowing clients to implement handlers to be notified about metadata
- Added `ResultSetExtender` to allow service implementations to register extensions of the `ResultSet` base interface, adding more data to the resultset base class.

See [documentation](docs/SERVICE-BUS.md#extending-the-response) for more details.

## [0.6.11] - 2024-05-30
### Improved
ARGUS-44166
- Improve logging of ServiceResponseContext when `debugOpenRequests` is enabled, including timestamp and thread name.
- Add metric and print warning logs for all connections older than a configurable limit `connectionAgeWarningLimit`.
- Add metrics to ServiceV1HttpClient for service invocations, service timeouts and gateway timeouts (as seen from client) for each priority port.
- Add metric to ServiceProxy to count usage of circuit breaker.

## [0.6.10] - 2024-05-28
### Added
ARGUS-44166
- Added option `debugOpenRequests` to log service/method of open SPI client requests on `ServiceV1HttpClient.getMetrics()`

## [0.6.9] - 2024-05-28
### Fixed
ARGUS-44160
- Ensure that ClientResultSet is closed when using `.iterator()`

## [0.6.8] - 2024-05-15
### Added
ARGUS-43963
- Added HTTP client pool metrics to the ServiceV1HttpClient

## [0.6.7] - 2024-05-03
### Changed
ARGUS-43696
- Add Serializer metrics in ServiceClient

## [0.6.6] - 2024-04-22
### Changed
ARGUS-43419
- Ensure closing of `ResultSet` in `ServiceInvocationHandler` after handling streaming result.
- Invoke `ResultSet.cancel()` if an error occurs during `ServiceInvocationHandler` handling of resultset

The `ServiceInvocationHandler` already closes the enclosing `ServiceSession` after handling a `ResultSet`.  
However, in some situations, the `ResultSet` may have its own session, so the handler must also close this resource.

## [0.6.5] - 2024-04-22
### Changed
ARGUS-43494
- Reduce excessive logging on checked exceptions in `ServiceInvocationHandler`.

## [0.6.4] - 2024-04-21
### Added
ARGUS-43191
- Implementing optional `CircuitBreakerHandler` in `ServiceProxy`.
- If enabled, this handler will early reject requests to Jetty if the server thread pool is (near) empty, 
instead of queueing requests. This will lead to rejecting requests early, instead returning 503,
allowing an upstream load balancer to forward the request to other instances.

## [0.6.3] - 2024-04-18
### Added
ARGUSUSER-7540 
- Ensure proper close of resources in ClientResultSet.
- Define new `Resource` extending `Closeable` with `cancel()`
- Extend ResultSet as `Resource`
- Ensure cancelling of underlying `Resource` when `ClientResultSet` is cancelled.

## [0.6.2] - 2024-04-17
### Added
ARGUS-43191 
- Add thread pool metrics in `ServiceProxy` 

## [0.6.1] - 2024-04-15
### Fixed
ARGUS-43192
- `ServiceV1HttpClient` throws `ServiceTimeoutException` on connect timeout.
- Added configuration option `ServiceV1HttpClient.setConnectionTimeoutSeconds(long)` to configure non-default connection timeout. 
Defaults to 3 minutes (HttpClient default). 

## [0.6.0] - 2024-03-20
### Changed
ARGUS-39520
- Upgraded to Jetty 10
- Note that this upgrade requires clients using the `service-proxy` to also move to Jetty10.

## [0.5.9] - 2024-03-20
### Changed
ARGUS-42621
- Flush response buffer on ServiceProxy keepalive, to avoid gateway timeout
- Use per-server configured ObjectMapper in ResultSetParser to configure max size for parsing large ResultSet response objects

## [0.5.8] - 2024-03-13
### Changed
ARGUS-42553
- Add `ServiceClient.setReadMaxStringLength(int)` and `ServiceProxy.setReadMaxStringLength(int)` 
to allow configuring max SPI document size

## [0.5.7] - 2024-03-12
### Changed
ARGUS-42524
- Improve error handling for gateway timeout

## [0.5.6] - 2024-03-11
### Changed
ARGUS-42345
- Added `ServiceClient.closeThreadResources()` to allow client threads to close HTTP client resources
which are dangling after SPI invocations. 
- Added implicit invocation of `ServiceClient.closeThreadResources()` at the end of every
service proxy server-side handling in `ServiceV1Servlet`, to ensure that any downstream clients are closed
when the handler is done.

## [0.5.3] - 2024-02-29
### Changed
ARGUS-42087
- The current serviceproxy implementation opens the request session in a different thread than the thread used to execute the actual method, causing the method invocation to open its own session instead, which is then not kept open after the method is done.
- This change runs the method execution using the request thread, and runs the keepalive generation in a separate thread.

## [0.5.2] - 2024-02-15
### Changed
ARGUS-41771 
- Fix resolving of primitive types in ServiceInvocationHandler

## [0.5.1] - 2023-01-28
### Changed
ARGUS-40424 
- Small improvements for API proxy

## [0.5.0] - 2023-01-28
### Changed
ARGUS-40424 
- Implement api proxy

## [0.4.0] - 2023-09-29
### Changed
ARGUSUSER-6576
- Upgraded messaging to 1.4.0.

### Upgrade notes
This upgrade introduces JMS RequestSink protocol V4, and removes support for protocol versions V1 and V2.
- Upgrading clients must ensure that service topic is configured and set.
- Upgrading clients _should_ enable protocol version V4, to enable flow control of streaming responses.
- For clients having V4 enabled, a new method `ServiceProxy.setNextResponseWindowSize(int)` allows 
client to set the response window size for the next request. This can be used to e.g. reduce memory pressure if each response segment
in the client invocation response is expected to be very large.

## [0.3.49] - 2023-03-22
### Changed
ARGUS-35165
- Renamed generic "errors" metric in ServiceMessageClient to "streamingInterrupted" to clearly indicate the purpose.

## [0.3.48] - 2023-03-03
### Fixed
ARGUS-33367
- Upgraded nexus-staging-maven-plugin in order to fix deployment to Maven Central.

## [0.3.47] - 2023.02.27
### Changed
ARGUS-32473
- Made project build with JDK17 (failed on javadoc generation).
- Upgraded dependencies to the newest minor/bugfix versions.
- Swapped `javax.inject` artefact to Jakarta.

## [0.3.46] - 2023.01.16
### Changed
ARGUS-33344
- Ensure `ServiceMessageHandler` closes `ResultSet` after processing 

## [0.3.45] - 2023.01.04
### Changed
ARGUS-32839
- Ensure `ServiceMessageClient` will invoke `RequestHandler.close()` when thread is interrupted.
- Ensure `ServiceMessageHandler` cancels/interrupts ongoing call future when receiving `abort()` 

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

