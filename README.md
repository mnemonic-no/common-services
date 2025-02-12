mnemonic common services
========================

*mnemonic common services* provides a set of reusable Java components for building scalable microservices. Following the Don't-Repeat-Yourself principle those components contain common functionality which we have written over the years for our services. We publish them as Open Source with the hope that they might be useful to others as well.

## Usage

[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/auth.svg?color=orange&label=auth)](https://javadoc.io/doc/no.mnemonic.services.common/auth)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/documentation.svg?color=orange&label=documentation)](https://javadoc.io/doc/no.mnemonic.services.common/documentation)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/hazelcast-consumer.svg?color=orange&label=hazelcast-consumer)](https://javadoc.io/doc/no.mnemonic.services.common/hazelcast-consumer)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/hazelcast5-consumer.svg?color=orange&label=hazelcast5-consumer)](https://javadoc.io/doc/no.mnemonic.services.common/hazelcast5-consumer)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/messagebus.svg?color=orange&label=messagebus)](https://javadoc.io/doc/no.mnemonic.services.common/messagebus)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/service-api.svg?color=orange&label=service-api)](https://javadoc.io/doc/no.mnemonic.services.common/service-api)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/service-proxy.svg?color=orange&label=service-proxy)](https://javadoc.io/doc/no.mnemonic.services.common/service-proxy)
[![Javadocs](https://javadoc.io/badge/no.mnemonic.services.common/service-proxy-jakarta.svg?color=orange&label=service-proxy-jakarta)](https://javadoc.io/doc/no.mnemonic.services.common/service-proxy-jakarta)

## Installation

All libraries provided by *mnemonic common services* are directly available from Maven Central. Just declare a dependency in your pom.xml and start using it:

```xml
<dependency>
  <groupId>no.mnemonic.services.common</groupId>
  <artifactId>${artifactId}</artifactId>
  <version>${version}</version>
</dependency>
```

Replace ${artifactId} and ${version} with the library and version you want to use.

## Requirements

None, dependencies will be handled by Maven automatically.

## Known issues

See [Issues](https://github.com/mnemonic-no/common-services/issues).

## Contributing

See the [CONTRIBUTING.md](CONTRIBUTING.md) file.

## License

*mnemonic common services* is released under the ISC License. See the bundled LICENSE file for details.