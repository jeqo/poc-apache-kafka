# `kfk-cluster-state` - Kafka CLI: cluster state

[![cli:cluster-state:build](https://github.com/jeqo/poc-apache-kafka/actions/workflows/cli-cluster-state-build.yml/badge.svg)](https://github.com/jeqo/poc-apache-kafka/actions/workflows/cli-cluster-state-build.yml)

Command-line tool to get a JSON representation of the topics included in a Kafka cluster.
Includes topic metadata, configuration, partitions, replica placement, and offsets.

This information is available through multiple commands, e.g. `kafka-topics`, `kafka-configs`, etc.
That's why I decided to compile it in one single tool.

- [Documentation](./docs/kfk-cluster-state.adoc)

## Output structure

- cluster
  - id
  - brokers
    - id
    - host
    - rack
- topics
  - name
  - topic
    - name
    - id
    - partitions
      - id
      - leader
      - replicas
      - isr
      - startOffset
        - offset
        - timestamp
        - leaderEpoch
      - endOffset
        - offset
        - timestamp
        - leaderEpoch
    - config
      - name
      - configEntry
        - name
        - value
        - isReadOnly
        - isSensitive
        - isDefault
        - documentation
        - synonyms

NOTE: Recommended using it with [jq](https://stedolan.github.io/jq/) and [jless](https://github.com/PaulJuliusMartinez/jless) to access JSON output.
