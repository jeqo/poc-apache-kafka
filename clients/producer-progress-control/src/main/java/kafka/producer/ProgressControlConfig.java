package kafka.producer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public record ProgressControlConfig(
    boolean onlyOnce,
    long start,
    long end,
    long backoff,
    boolean backoffExponential,
    Set<String> topicsIncluded) {

  public static final String START_MS_CONFIG = "progress.control.start.ms";
  public static final String END_MS_CONFIG = "progress.control.end.ms";
  public static final String BACKOFF_MS_CONFIG = "progress.control.backoff.ms";
  public static final long BACKOFF_MS_DEFAULT = 1_000;
  public static final String BACKOFF_EXPONENTIAL_CONFIG = "progress.control.backoff.exponential";
  public static final boolean BACKOFF_EXPONENTIAL_DEFAULT = false;

  public static final String TOPICS_INCLUDE_CONFIG = "progress.control.topics.include";

  static ProgressControlConfig load(Map<String, ?> props) {
    var builder = newBuilder();
    if (props.containsKey(START_MS_CONFIG)) {
      var start = Duration.ofMillis(Long.parseLong(props.get(START_MS_CONFIG).toString()));
      builder.withStart(start);
    }
    if (props.containsKey(END_MS_CONFIG)) {
      var end = Duration.ofMillis(Long.parseLong(props.get(END_MS_CONFIG).toString()));
      var backoff =
          props.containsKey(BACKOFF_MS_CONFIG)
              ? Duration.ofMillis(Long.parseLong(props.get(BACKOFF_MS_CONFIG).toString()))
              : Duration.ofMillis(BACKOFF_MS_DEFAULT);
      var exp =
          props.containsKey(BACKOFF_EXPONENTIAL_CONFIG)
              ? Boolean.parseBoolean(props.get(BACKOFF_EXPONENTIAL_CONFIG).toString())
              : BACKOFF_EXPONENTIAL_DEFAULT;
      builder.withEnd(end, backoff, exp);
    }
    if (props.containsKey(TOPICS_INCLUDE_CONFIG)) {
      var topicsInclude = props.get(TOPICS_INCLUDE_CONFIG).toString();
      builder.withTopicsIncluded(topicsInclude.split(","));
    }
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    boolean onlyOnce = true;
    Duration start = Duration.ofSeconds(10);
    long end = -1;
    Duration backoff = Duration.ofSeconds(1);
    boolean exponential = true;
    Set<String> topicsIncluded = new HashSet<>();

    Builder withStart(Duration start) {
      if (Long.compare(end, start.toMillis()) > 1) {
        throw new IllegalArgumentException("end <= start");
      }
      if (Long.compare(end, start.plus(backoff).toMillis()) > 1) {
        throw new IllegalArgumentException("end <= start + backoff");
      }
      this.start = start;
      return this;
    }

    Builder withEnd(Duration end, Duration backoff) {
      return withEnd(end, backoff, false);
    }

    Builder withEnd(Duration end, Duration backoff, boolean exponential) {
      if (end.compareTo(start) <= 0) {
        throw new IllegalArgumentException("end <= start");
      }
      if (end.compareTo(start.plus(backoff)) <= 0) {
        throw new IllegalArgumentException("end <= start + backoff");
      }
      this.end = end.toMillis();
      this.onlyOnce = false;
      this.backoff = backoff;
      this.exponential = exponential;
      return this;
    }

    Builder withTopicsIncluded(String[] topicsIncluded) {
      for (var t : topicsIncluded) {
        if (!t.isBlank()) {
          this.topicsIncluded.add(t);
        }
      }
      return this;
    }

    public ProgressControlConfig build() {
      return new ProgressControlConfig(
          onlyOnce, start.toMillis(), end, backoff.toMillis(), exponential, topicsIncluded);
    }
  }
}
