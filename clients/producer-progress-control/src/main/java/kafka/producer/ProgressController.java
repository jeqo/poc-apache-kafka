package kafka.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;

// sleep when no progress?
// restart when new partitions are added?
public class ProgressController implements Closeable {
  final Config config;
  Map<TopicPartition, Control> progress;

  volatile boolean running;

  public ProgressController(Config config) {
    this.config = config;
    this.progress = new ConcurrentHashMap<>();
  }

  void control() {
    while (running) {
      progress.forEach(
          (tp, control) -> {
            long current = System.currentTimeMillis();
            if (eval(tp, control, current)) {
              sendControl();
            }
          });
    }
  }

  boolean eval(TopicPartition tp, Control control, long current) {
    long diff = current - control.started();
    var iteration = control.iteration(); //iterations.getOrDefault(tp, 0);
    if (diff > config.start() + (Math.pow(2, iteration) * config.backoff())) {
      if (config.onlyOnce() || diff >= config.end()) {
        progress.remove(tp);
      } else {
        progress.put(tp, control.increment(current));
      }
      return true;
    }
    return false;
  }

  public void addTopicPartition(String topic, int partition, long timestamp) {
    final var tp = new TopicPartition(topic, partition);
    addTopicPartition(tp, timestamp);
  }

  public void addTopicPartition(TopicPartition tp, long ts) {
    this.progress.put(tp, Control.create(ts));
  }

  void sendControl() {
    //TODO send message
  }

  @Override
  public void close() throws IOException {
    this.running = false;
  }


  record Control(long started, long latest, long iteration) {

    public static Control create(long timestamp) {
      return new Control(timestamp, timestamp, 0);
    }

    public Control increment(long timestamp) {
      return new Control(started, timestamp, this.iteration + 1);
    }
  }

  record Config(boolean onlyOnce, long start, long end, long backoff, boolean backoffExponential) {
    public static Builder newBuilder() {
      return new Builder();
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
