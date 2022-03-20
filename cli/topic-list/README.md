# `kfk-topic-list` - Kafka CLI: topic list

Command-line tool to get a JSON representation of the topics included in a Kafka cluster.
Includes topic metadata, configuration, partitions, replica placement, and offsets.

This information is available through multiple commands, e.g. `kafka-topics`, `kafka-configs`, etc.
That's why I decided to compile it in one single tool.

- [Documentation](./docs/kfk-topic-list.adoc)

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
