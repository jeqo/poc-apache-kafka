# Apache Kafka CLIs

This module includes some experimental CLIs for Apache Kafka.

## Modules

### Topics 

#### ListTopics

The `list-topics` CLI does a bit more than default `kafka-topics` by also including `offset` information.

### Producer

#### Performance - DataGen

The `producer-perf-datagen` extends the default `ProducerPerformance` by combining the data-generation with the library behind [Datagen Source Connector]()