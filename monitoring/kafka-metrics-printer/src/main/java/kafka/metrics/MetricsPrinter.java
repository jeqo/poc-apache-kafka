package kafka.metrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.Metric;

public class MetricsPrinter {

  final Supplier<Map<org.apache.kafka.common.MetricName, ? extends Metric>> metricsSupplier;
  final List<MetricName> metricNames;
  final Map<String, List<String>> metricNamesByGroup;
  final long frequency;

  public MetricsPrinter(
      Supplier<Map<org.apache.kafka.common.MetricName, ? extends Metric>> ks,
      List<MetricName> metricNames,
      long frequency
  ) {
    this.metricsSupplier = ks;
    this.metricNames = metricNames;
    this.frequency = frequency;
    final Map<String, List<String>> byGroup = new HashMap<>();
    for (MetricName m : metricNames) {
      byGroup.computeIfPresent(m.group, (s, names) -> {
        names.add(m.name);
        return names;
      });
      byGroup.computeIfAbsent(m.group, s -> {
        final List<String> groups = new ArrayList<>();
        groups.add(m.name);
        return groups;
      });
    }
    this.metricNamesByGroup = new HashMap<>(byGroup);
  }

  public void start() throws InterruptedException {
    final Set<org.apache.kafka.common.MetricName> metricNames = metricsSupplier.get().keySet();
    System.out.println("All metrics:");
    metricNames.forEach(System.out::println);
    run();
  }

  List<org.apache.kafka.common.MetricName> selected = new ArrayList<>();

  public void run() throws InterruptedException {
    runOnce();

    Thread.sleep(frequency);
    run();
  }

  private void runOnce() {
    final Map<org.apache.kafka.common.MetricName, ? extends Metric> metrics = metricsSupplier.get();
    final Set<org.apache.kafka.common.MetricName> metricNames = metrics.keySet();
    final List<org.apache.kafka.common.MetricName> selected = metricNames.stream()
        .filter(metricName -> metricNamesByGroup.containsKey(metricName.group()))
        .filter(metricName -> metricNamesByGroup.get(metricName.group())
            .contains(metricName.name()))
        .collect(Collectors.toList());
    if (selected.size() != this.selected.size()) {
      this.selected = selected;
      System.out.println("Selected metrics:");
      selected.forEach(System.out::println);
    }
    try {
      for (org.apache.kafka.common.MetricName metricName : selected) {
        Metric m = metrics.get(metricName);
        System.out.println(Instant.now() + " @ "
            + m.metricName().group() + ":" + m.metricName().name()
            + m.metricName().tags() +
            " => " + m.metricValue());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println();
  }

  public static class MetricName {
    final String group;
    final String name;

    public MetricName(String group, String name) {
      this.group = group;
      this.name = name;
    }

    public String group() {
      return group;
    }

    public String name() {
      return name;
    }
  }
}
