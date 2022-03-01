# Apache Kafka CLIs

This module includes some experimental CLIs for Apache Kafka.

## CLIs

### Topics 

#### ListTopics

The `kafka-list-topics` CLI does a bit more than default `kafka-topics` by also including `offset` information.

### Producer

#### Performance - DataGen

The `kafka-producer-datagen` extends the default `ProducerPerformance` by combining the data-generation with the library behind [Datagen Source Connector](https://github.com/confluentinc/kafka-connect-datagen).
