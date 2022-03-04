package io.github.jeqo.kafka;

public class Config {
    final Kafka kafka;
    final String connectGroupPrefix;
    final long maxLag;
    //    final Duration frequency;
    final KafkaConnect kafkaConnect;

    public Config(Kafka kafka,
                  String connectGroupPrefix,
                  long maxLag,
                  //Duration frequency,
                  KafkaConnect kafkaConnect) {
        this.kafka = kafka;
        this.connectGroupPrefix = connectGroupPrefix;
        this.maxLag = maxLag;
        //this.frequency = frequency;
        this.kafkaConnect = kafkaConnect;
    }

    @Override
    public String toString() {
        return "Config{" +
                "kafka=" + kafka +
                ", connectGroupPrefix='" + connectGroupPrefix + '\'' +
                ", maxLag=" + maxLag +
                ", kafkaConnect=" + kafkaConnect +
                '}';
    }

    static class Kafka {
        final String bootstrapServers;

        Kafka(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        @Override
        public String toString() {
            return "Kafka{" +
                    "bootstrapServers='" + bootstrapServers + '\'' +
                    '}';
        }
    }

    static class KafkaConnect {
        final String url;

        KafkaConnect(String url) {
            this.url = url;
        }

        @Override
        public String toString() {
            return "KafkaConnect{" +
                    "url='" + url + '\'' +
                    '}';
        }
    }
}
