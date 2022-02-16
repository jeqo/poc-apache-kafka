package poc.metrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KafkaStreams;

public class MetricsPrinter {

  final KafkaStreams ks;
  final List<MetricName> metricNames;
  final Map<String, List<String>> metricNamesByGroup;
  final long frequency;

  public MetricsPrinter(
      KafkaStreams ks,
      List<MetricName> metricNames,
      long frequency) {
    this.ks = ks;
    this.metricNames = metricNames;
    this.frequency = frequency;
    var byGroup = new HashMap<String, List<String>>();
    for (var m : metricNames) {
      byGroup.computeIfPresent(m.group, (s, names) -> {
        names.add(m.name);
        return names;
      });
      byGroup.computeIfAbsent(m.group, s -> {
        var groups = new ArrayList<String>();
        groups.add(m.name);
        return groups;
      });
    }
    this.metricNamesByGroup = new HashMap<>(byGroup);
  }

  public void start() throws InterruptedException {
    final var metricNames = ks.metrics().keySet();
    System.out.println("All metrics:");
    metricNames.forEach(System.out::println);
    run();
  }

  List<org.apache.kafka.common.MetricName> selected = new ArrayList<>();

  public void run() throws InterruptedException {
    final var metrics = ks.metrics();
    final var metricNames = metrics.keySet();
    final var selected = metricNames.stream()
        .filter(metricName -> metricNamesByGroup.containsKey(metricName.group()))
        .filter(metricName -> metricNamesByGroup.get(metricName.group())
            .contains(metricName.name()))
        .toList();
    if (selected.size() != this.selected.size()) {
      this.selected = selected;
      System.out.println("Selected metrics:");
      selected.forEach(System.out::println);
    }
    try {
      for (var metricName : selected) {
         var m = metrics.get(metricName);
        System.out.println(Instant.now() + " @ " + m.metricName().group() + ":" + m.metricName().name() + " => " + m.metricValue());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    Thread.sleep(frequency);
    run();
  }

  public record MetricName(
      String name,
      String group
  ) {

  }
}
