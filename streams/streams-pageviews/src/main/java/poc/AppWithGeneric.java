package poc;

import java.util.Properties;

public class AppWithGeneric {

  public static void main(String[] args) {
    final var configs = new Properties();
    configs.put("bootstrap.servers", "localhost:9092");
  }
}
