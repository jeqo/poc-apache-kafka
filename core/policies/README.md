# Kafka Policies

Kafka has support for applying policies when some action is requested (e.g. create topic, alter config).
These functionalities are defined in: 

- KIP-108: https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy
  - Example: [source code](src/main/java/poc/kafka/policy/AppTopicCreationPolicy.java)
- KIP-133: https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs
  - Example: [source code](src/main/java/poc/kafka/policy/AppAlterTopicConfigPolicy.java)

## TODO

- [ ] Add documentation of policies to Kafka ops docs.