package kafka.datagen;

import io.confluent.avro.random.generator.Generator;
import java.io.FileNotFoundException;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;

/**
 * Datagen side
 */
public class PayloadGenerator implements Supplier<byte[]> {

  final Config config;
  final Random random;
  final Generator generator;

  public PayloadGenerator(Config config) {
    this.config = config;

    this.random = new Random();
    if (config.randomSeed() != null) {
      random.setSeed(config.randomSeed());
      random.setSeed(random.nextLong());
    }

    generator = new Generator.Builder()
        .random(random)
        .generation(config.count())
        .schema(config.schema())
        .build();
  }

  @Override
  public byte[] get() {
    final Object generatedObject = generator.generate();
    if (!(generatedObject instanceof GenericRecord)) {
      throw new RuntimeException(String.format(
          "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
          generatedObject.getClass().getName()
      ));
    }
    final GenericRecord randomAvroMessage = (GenericRecord) generatedObject;

    return new byte[0];
  }

  record Config(Long randomSeed, Optional<String> quickstart, Optional<Path> schemaPath,
                Optional<String> schemaString, long count) {

    Schema schema() {
      return quickstart.map(s -> Quickstart.valueOf(s.toUpperCase()).getSchemaFilename())
          .map(Config::getSchemaFromSchemaFileName)
          .orElse(
              schemaString.map(Config::getSchemaFromSchemaString).orElse(
                  schemaPath.map(s -> getSchemaFromSchemaFileName(schemaPath.get())).orElse(null)
              )
          );
    }

    public static Schema getSchemaFromSchemaString(String schemaString) {
      Schema.Parser schemaParser = new Parser();
      Schema schema;
      try {
        schema = schemaParser.parse(schemaString);
      } catch (SchemaParseException e) {
        // log.error("Unable to parse the provided schema", e);
        throw new ConfigException("Unable to parse the provided schema");
      }
      return schema;
    }

    public static Schema getSchemaFromSchemaFileName(Path schemaFileName) {
      Schema.Parser schemaParser = new Parser();
      Schema schema;
      try (InputStream stream = Files.newInputStream(schemaFileName)) {
        schema = schemaParser.parse(stream);
      } catch (FileNotFoundException e) {
        try {
          if (Files.isRegularFile(schemaFileName)) {
            throw new ConfigException("Unable to find the schema file");
          }
          schema = schemaParser.parse(
              Config.class.getClassLoader().getResourceAsStream(schemaFileName.toString())
          );
        } catch (SchemaParseException | IOException i) {
          // log.error("Unable to parse the provided schema", i);
          throw new ConfigException("Unable to parse the provided schema");
        }
      } catch (SchemaParseException | IOException e) {
        // log.error("Unable to parse the provided schema", e);
        throw new ConfigException("Unable to parse the provided schema");
      }
      return schema;
    }
  }
}
