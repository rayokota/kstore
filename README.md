# KStore - A Wide Column Store Backed by Apache Kafka

[![Build Status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[travis-shield]: https://travis-ci.org/rayokota/kstore.svg?branch=master
[travis-link]: https://travis-ci.org/rayokota/kstore
[maven-shield]: https://img.shields.io/maven-central/v/io.kstore/kstore.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.kstore
[javadoc-shield]: https://javadoc.io/badge/io.kstore/kstore.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kstore/kstore

KStore is a client library that provides a wide column store (or extensible record store) abstraction for Kafka.  It implements the HBase client API and can be used as a drop-in replacement for HBase.

## Maven

Releases of KStore are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.kstore</groupId>
    <artifactId>kstore</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

KStore can be used by configuring the value for `hbase.client.connection.impl` in `hbase-site.xml`. 

```
<configuration>
    <property>
        <name>hbase.client.connection.impl</name>
        <value>io.kstore.KafkaStoreConnection</value>
    </property>
    <property>
        <name>kafkacache.bootstrap.servers</name>
        <value>localhost:9092</value>
    </property>
</configuration>
```

Internally KStore uses [KCache](https://github.com/rayokota/kcache) and so all the configuration properties of KCache can be used.  See the [KCache documentation](https://github.com/rayokota/kcache#basic-configuration) for further details.


See [this post](https://yokota.blog/2020/01/13/building-a-graph-database-using-kafka/) for more examples of how to use KStore.
