package kafka.producer;

import java.time.Duration;
import java.util.Map;

public class ProgressControlConfig {
  public static final String START_MS_CONFIG = "progress.control.start.ms";
  public static final String END_MS_CONFIG = "progress.control.end.ms";
  public static final String BACKOFF_MS_CONFIG = "progress.control.backoff.ms";
  public static final long BACKOFF_MS_DEFAULT = 1_000;
  public static final String BACKOFF_EXPOTENTIAL_CONFIG = "progress.control.backoff.exponential";
  public static final boolean BACKOFF_EXPOTENTIAL_DEFAULT = false;

  static ProgressController.Config load(Map<String, ?> props) {
    var builder = ProgressController.Config.newBuilder();
    if (props.containsKey(START_MS_CONFIG)) {
      var start = Duration.ofMillis(Long.parseLong(props.get(START_MS_CONFIG).toString()));
      builder.withStart(start);
    }
    if (props.containsKey(END_MS_CONFIG)) {
      var end = Duration.ofMillis(Long.parseLong(props.get(END_MS_CONFIG).toString()));
      var backoff = props.containsKey(BACKOFF_MS_CONFIG)
              ? Duration.ofMillis(Long.parseLong(props.get(BACKOFF_MS_CONFIG).toString()))
              : Duration.ofMillis(BACKOFF_MS_DEFAULT);
      var exp = props.containsKey(BACKOFF_EXPOTENTIAL_CONFIG)
              ? Boolean.parseBoolean(props.get(BACKOFF_EXPOTENTIAL_CONFIG).toString())
              : BACKOFF_EXPOTENTIAL_DEFAULT;
      builder.withEnd(end, backoff, exp);
    }
    return builder.build();
  }
}
