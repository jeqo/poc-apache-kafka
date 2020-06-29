package poc;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.ValueAndHeaders;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueAndHeadersSerde;

public class Main {
    public static void main(String[] args) {
        var builder = new StreamsBuilder();
        builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
                .withHeaders()
                .filter((k, v) -> v.headers().headers("k").iterator().hasNext())
                .filter((k, v) -> Arrays.equals(v.headers().lastHeader("k").value(), "v".getBytes()))
                .groupByKey(Grouped.with(Serdes.String(), new ValueAndHeadersSerde<>(Serdes.String())))
                .reduce((oldValue, newValue) -> {
                    newValue.headers().add("reduced", "yes".getBytes());
                    return new ValueAndHeaders<>(oldValue.value().concat(newValue.value()), newValue.headers());
                })
                .toStream()
                .setHeaders((k, v) -> v.headers())
                .mapValues((k, v) -> v.value())
                .setHeader((k, v) -> new RecordHeader("newH", "1".getBytes()))
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        var configs = new Properties();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(APPLICATION_ID_CONFIG, "kip-headers");
        var kafkaStreams = new KafkaStreams(builder.build(), configs);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        kafkaStreams.start();
    }
}
