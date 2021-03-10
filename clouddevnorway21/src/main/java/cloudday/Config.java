package cloudday;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;

public class Config {

  public static Properties common() throws IOException {
    var configs = new Properties();
    configs.load(Files.newBufferedReader(Path.of("ccloud.properties")));
    return configs;
  }
}
