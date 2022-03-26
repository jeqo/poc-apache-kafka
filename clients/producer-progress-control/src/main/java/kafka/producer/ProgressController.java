package kafka.producer;

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;

// sleep when no progress?
// restart when new partitions are added?
public class ProgressController {
  final Config config;
  Map<TopicPartition, Long> progress;
  Map<TopicPartition, Integer> iterations;

  public ProgressController(Config config) {
    this.config = config;
  }

  void control() {
    while (true) {
      progress.forEach(
          (tp, latest) -> {
            long current = System.currentTimeMillis();
            long diff = current - latest;
            eval(tp, current, diff);
          });
    }
  }

  void eval(TopicPartition tp, long current, long diff) {
    var iteration = iterations.getOrDefault(tp, 0);
    if (config.shouldSendControl(diff, iteration)) {
      sendControl();
      if (config.isLastOne(diff)) {
        progress.remove(tp);
        iterations.remove(tp);
      } else {
        progress.put(tp, current);
        iterations.put(tp, ++iteration);
      }
    }
  }

  void sendControl() {

  }

  record Config(boolean onlyOnce, long start, long end, long backoff, boolean backoffExponential) {
    public static Builder newBuilder() {
      return new Builder();
    }

    public boolean isLastOne(long diff) {
      return onlyOnce || diff >= end;
    }

    boolean shouldSendControl(long diff) {
      return shouldSendControl(diff, 0L);
    }

    boolean shouldSendControl(long diff, long iteration) {
      return diff > start + (Math.pow(2, iteration) * this.backoff);
    }

    static class Builder {
      boolean onlyOnce = true;
      Duration start = Duration.ofSeconds(10);
      long end = -1;
      Duration backoff = Duration.ofSeconds(0);
      boolean exponential = true;

      Builder withEnd(Duration end) {
        if (end.compareTo(start) > 0) throw new IllegalArgumentException("end <= start");
        if (end.compareTo(start.plus(backoff)) > 0)
          throw new IllegalArgumentException("end <= start + backoff");
        this.end = end.toMillis();
        this.onlyOnce = false;
        return this;
      }

      Builder withStart(Duration start) {
        if (Long.compare(end, start.toMillis()) > 1) throw new IllegalArgumentException("end <= start");
        if (Long.compare(end, start.plus(backoff).toMillis()) > 1)
          throw new IllegalArgumentException("end <= start + backoff");
        this.start = start;
        return this;
      }

      Builder withBackoff(Duration backoff, boolean exponential) {
        this.backoff = backoff;
        this.exponential = exponential;
        return this;
      }

      public Config build() {
        return new Config(onlyOnce, start.toMillis(), end, backoff.toMillis(), exponential);
      }
    }
  }
}
