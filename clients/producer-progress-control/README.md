# Kafka Producer: Progress Control

This Kafka Producer instrumentation is meant to introduce the concept of control messages to be emitted when no messages have been sent to topic partitions.

## Context

Streaming applications depend on event-time moving forward.
This time is defined by new messages arriving to topic partitions.

For instance, when a session window is created in Kafka Streams, to close a window a new message needs to arrive after the session window timeout (i.e. inactivity gap period) has passed.

## Goals

- [x] Keep track of messages sent per topic-partition
- [x] Send control messages after a timeout period has passed
  - [x] Allow to define a back-off period
  - [x] Allow to define a maximum consecutive messages
- [x] Document how consumers/streaming applications could use control messages.
