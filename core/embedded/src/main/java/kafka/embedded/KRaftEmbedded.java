package kafka.embedded;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.MetaProperties;
import kafka.tools.StorageTool;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.SystemTime;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

public class KRaftEmbedded {

  public static void main(String[] args) {
    var props = new Properties();
    props.put(KafkaConfig.ProcessRolesProp(), "controller,broker");
    props.put(KafkaConfig.BrokerIdProp(), "101");
    final var nodeId = 101;
    props.put("node.id", nodeId);

    props.put(KafkaConfig.ListenerSecurityProtocolMapProp(), "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
    props.put(KafkaConfig.ListenersProp(), "CONTROLLER://localhost:9091,PLAINTEXT://0.0.0.0:9092");
    props.put(KafkaConfig.AdvertisedListenersProp(), "PLAINTEXT://localhost:9092");
    props.put(KafkaConfig.ControllerListenerNamesProp(), "CONTROLLER");
    props.put(KafkaConfig.InterBrokerListenerNameProp(), "PLAINTEXT");
    props.put("controller.quorum.voters", nodeId + "@localhost:9091");

    final var logDirs = "/tmp/kraft-combined-logs_2";
    if (!Files.isDirectory(Path.of(logDirs))) {
      var uuid = Uuid.randomUuid();
      StorageTool.formatCommand(
        System.out,
        Seq.from(CollectionConverters.asScala(List.of(logDirs))),
        new MetaProperties(uuid.toString(), nodeId),
        true
      );
    }
    props.put(KafkaConfig.LogDirsProp(), logDirs);

    var config = KafkaConfig.fromProps(props, true);
    // from https://github.com/apache/kafka/blob/16324448a2d3d52f6a3a0a2058c3ceec14e84d52/core/src/main/scala/kafka/Kafka.scala#L76-L80
    var kraftServer = new KafkaRaftServer(config, SystemTime.SYSTEM, Option.empty());
    Runtime.getRuntime().addShutdownHook(new Thread(kraftServer::shutdown));
    kraftServer.startup();
  }
}
