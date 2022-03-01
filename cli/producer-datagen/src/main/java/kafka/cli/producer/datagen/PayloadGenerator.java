package kafka.cli.producer.datagen;

import io.confluent.avro.random.generator.Generator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;

/**
 * Datagen side
 */
public class PayloadGenerator implements Supplier<GenericRecord> {

    final Config config;
    final Random random;
    final Generator generator;
    final String keyFieldName;

    public PayloadGenerator(Config config) {
        this.config = config;

        this.random = new Random();
        config.randomSeed().ifPresent(r -> {
            random.setSeed(r);
            random.setSeed(random.nextLong());
        });

        generator = new Generator.Builder()
                .random(random)
                .generation(config.count())
                .schema(config.schema())
                .build();
        keyFieldName = config.keyFieldName();
    }

    @Override
    public GenericRecord get() {
        final Object generatedObject = generator.generate();
        if (!(generatedObject instanceof GenericRecord)) {
            throw new RuntimeException(String.format(
                    "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
                    generatedObject.getClass().getName()
            ));
        }
        return (GenericRecord) generatedObject;
    }

    byte[] sample() throws IOException {
        var record = get();
        var outputStream = new ByteArrayOutputStream();
        var schema = record.getSchema();
        var datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        return outputStream.toByteArray();
    }

    public String key(GenericRecord payload) {
        return String.valueOf(payload.get(config.keyFieldName()));
    }

    record Config(
            Optional<Long> randomSeed,
            Optional<Quickstart> quickstart,
            Optional<Path> schemaPath,
            Optional<String> schemaString,
            long count
    ) {

        Schema schema() {
            return quickstart.map(Quickstart::getSchemaFilename)
                    .map(Config::getSchemaFromSchemaFileName)
                    .orElse(
                            schemaString.map(Config::getSchemaFromSchemaString).orElse(
                                    schemaPath.map(s -> {
                                        Schema schemaFromSchemaFileName = null;
                                        try {
                                            schemaFromSchemaFileName = getSchemaFromSchemaFileName(Files.newInputStream(schemaPath.get()));
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                        return schemaFromSchemaFileName;
                                    }).orElse(null)
                            )
                    );
        }

        public static Schema getSchemaFromSchemaString(String schemaString) {
            Schema.Parser schemaParser = new Parser();
            Schema schema;
            try {
                schema = schemaParser.parse(schemaString);
            } catch (SchemaParseException e) {
                throw new ConfigException("Unable to parse the provided schema");
            }
            return schema;
        }

        public static Schema getSchemaFromSchemaFileName(InputStream stream) {
            Schema.Parser schemaParser = new Parser();
            Schema schema;
            try {
                schema = schemaParser.parse(stream);
            } catch (SchemaParseException | IOException e) {
                throw new ConfigException("Unable to parse the provided schema", e);
            } finally {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return schema;
        }

        public String keyFieldName() {
            return quickstart.map(Quickstart::getSchemaKeyField).orElse(null);
        }
    }

    public static void main(String[] args) throws IOException {
        var pg = new PayloadGenerator(new PayloadGenerator.Config(
                Optional.empty(),
                Optional.of(Quickstart.CLICKSTREAM),
                Optional.empty(),
                Optional.empty(),
                1000000));
        var bytes = pg.sample();
        Files.write(Path.of("test.avro"), bytes);
    }
}
