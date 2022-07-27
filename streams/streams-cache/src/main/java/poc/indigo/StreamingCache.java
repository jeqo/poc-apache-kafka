package poc.indigo;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import poc.indigo.error.MaxFailuresUncaughtExceptionHandler;
import poc.jsonschema2json.Signal;

/**
 * Application to showcase how to expose cached state created by a Kafka Streams application
 * from an HTTP endpoint.
 * <p>
 *
 */
public class StreamingCache {

  public static void main(String[] args) throws IOException {
    // Load properties from config file
    var name = "src/main/resources/streams";
    final var streamsProps = new Properties();
    if (args.length >= 1) {
      name = name + "_" + args[0];
    }
    final var configFile = name + ".properties";
    try (final var inputStream = new FileInputStream(configFile)) {
      System.out.printf("Loading config file %s%n", configFile);
      streamsProps.load(inputStream);
    }
    final var port = Integer.parseInt(streamsProps.getProperty("app.port"));
    final var hostname = "localhost"; //TODO replace by var with hostname System.getenv().get("HOSTNAME");
    final var appServer = hostname + ":" + port;
    streamsProps.put(StreamsConfig.APPLICATION_SERVER_CONFIG, appServer);

    var srClient = new CachedSchemaRegistryClient(
      List.of(streamsProps.getProperty("schema.registry.url")),
      10_000,
      List.of(new JsonSchemaProvider()),
      Map.of()
    );
    final var config = new LinkedHashMap<String, Object>();
    streamsProps.forEach((o, o2) -> config.put(o.toString(), o2));

    var valueSerde = new KafkaJsonSchemaSerde<>(srClient);
    valueSerde.configure(config, false);
    var outputValueSerde = new KafkaJsonSchemaSerde<Signal>(srClient);
    outputValueSerde.configure(config, false);

    // Define a Kafka Streams topology
    final var cacheTopology = new CacheTopology(valueSerde, outputValueSerde);
    final var topology = cacheTopology.get();
    System.out.println(topology.describe());

    // Start Kafka Streams application
    final var ks = new KafkaStreams(topology, streamsProps);
    // assign uncaught exception handler before starting the application
    ks.setUncaughtExceptionHandler(new MaxFailuresUncaughtExceptionHandler(3, 600_000));
    ks.start();

    final var storeName = "window-cache";

    final var app = Javalin
      .create()
      .get("/", ctx -> ctx.result("OK"))
      .get(
        "/internal/store/:key",
        ctx -> {
          var key = ctx.pathParam("key");
          getKeyFromStore(ks, storeName, ctx, key);
        }
      )
      .get(
        "/liveness",
        ctx -> {
          if (ks.state().isRunningOrRebalancing()) {
            ctx.result("Healthy");
          } else {
            ctx.result("Unhealthy");
          }
        }
      )
      .get(
        "/ready",
        ctx -> {
          if (ks.state() == State.RUNNING) {
            ctx.result("READY");
          } else {
            ctx.result("NOT READY");
          }
        }
      )
      .get("/topology", ctx -> ctx.result(topology.describe().toString()))
      .get(
        "/store/:key",
        ctx -> {
          var key = ctx.pathParam("key");
          var meta = ks.queryMetadataForKey(storeName, key, new StringSerializer());
          var activeHost = meta.activeHost();
          final var activeAppServer = activeHost.host() + ":" + activeHost.port();
          if (!appServer.equals(activeAppServer)) {
            final var url = "http://" + activeHost.host() + ":" + activeHost.port() + "/internal/store/" + key;
            System.out.printf("Redirecting to: %s%n", url);
            var response = HttpClient
              .newHttpClient()
              .send(HttpRequest.newBuilder().GET().uri(URI.create(url)).build(), BodyHandlers.ofByteArray());
            ctx.status(response.statusCode()).result(response.body());
          } else {
            getKeyFromStore(ks, storeName, ctx, key);
          }
        }
      );
    app.start(port); // start HTTP server

    // Shutdown hook to close applications properly
    Runtime
      .getRuntime()
      .addShutdownHook(
        new Thread(() -> {
          app.stop();
          ks.close();
        })
      );
  }

  private static void getKeyFromStore(KafkaStreams ks, String storeName, Context ctx, String key) {
    ReadOnlyWindowStore<String, Object> cache = ks.store(
      StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore())
    );
    var now = Instant.now();
    try (
      WindowStoreIterator<Object> iterator = cache.backwardFetch(
        key,
        now.minusMillis(Duration.ofDays(1).toMillis()),
        now
      )
    ) {
      if (iterator.hasNext()) {
        var kv = iterator.next();
        final var resultString = "Found: " + kv.value + "@" + Instant.ofEpochMilli(kv.key);
        ctx.result(resultString);
      } else {
        ctx.status(404).result("Not found");
      }
    }
  }
}
