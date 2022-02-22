package kafka.producer.performance.datagen;

import picocli.CommandLine;

public class App {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Cli()).execute(args);
        System.exit(exitCode);
    }

//    private static void build() {

//        // --- Confluent ---
//        try {
//            Class.forName("io.confluent.kafka.serializers.KafkaAvroDeserializer", false,
//                    Thread.currentThread().getContextClassLoader());
//            reflectiveClass
//                    .produce(new ReflectiveClassBuildItem(true, false,
//                            "io.confluent.kafka.serializers.KafkaAvroDeserializer",
//                            "io.confluent.kafka.serializers.KafkaAvroSerializer"));
//
//            reflectiveClass
//                    .produce(new ReflectiveClassBuildItem(true, false, false,
//                            "io.confluent.kafka.serializers.context.NullContextNameStrategy"));
//
//            reflectiveClass
//                    .produce(new ReflectiveClassBuildItem(true, true, false,
//                            "io.confluent.kafka.serializers.subject.TopicNameStrategy",
//                            "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
//                            "io.confluent.kafka.serializers.subject.RecordNameStrategy"));
//
//            reflectiveClass
//                    .produce(new ReflectiveClassBuildItem(true, true, false,
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.Schema",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.Config",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTypeConverter",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.SujectVersion"));
//
//            reflectiveClass
//                    .produce(new ReflectiveClassBuildItem(true, true, false,
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest",
//                            "io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse"));
//        } catch (ClassNotFoundException e) {
//            //ignore, Confluent Avro is not in the classpath
//        }
//
//        try {
//            Class.forName("io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider", false,
//                    Thread.currentThread().getContextClassLoader());
//            serviceProviders
//                    .produce(new ServiceProviderBuildItem(
//                            "io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider",
//                            "io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider",
//                            "io.confluent.kafka.schemaregistry.client.security.basicauth.UrlBasicAuthCredentialProvider",
//                            "io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider"));
//        } catch (ClassNotFoundException e) {
//            // ignore, Confluent schema registry client not in the classpath
//        }    }
}
