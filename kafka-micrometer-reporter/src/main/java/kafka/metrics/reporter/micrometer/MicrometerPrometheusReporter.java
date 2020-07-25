package kafka.metrics.reporter.micrometer;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicrometerPrometheusReporter implements MetricsReporter, ClusterResourceListener, KafkaMetricsReporter {

    //    static final String KAFKA_VERSION_TAG_NAME = "kafka-version";
    static final String METRIC_NAME_PREFIX = "kafka.";
    static final Logger LOG = LoggerFactory.getLogger(MicrometerPrometheusReporter.class);

    static final String METRIC_GROUP_APP_INFO = "app-info";
    static final String METRIC_GROUP_METRICS_COUNT = "kafka-metrics-count";

    PrometheusMeterRegistry registry;
    Server server;
    int metricsPort;

//    private static final MetricsRegistry METRICS_REGISTRY = KafkaYammerMetrics.defaultRegistry();

    @Override
    public void configure(Map<String, ?> configs) {

        int metricsPort = 1234;
        if (configs.containsKey("metrics.reporter.micrometer.prometheus.port")) {
            Object metricsPortValue = configs.get("metrics.reporter.micrometer.prometheus.port");
            metricsPort = Integer.parseInt(metricsPortValue.toString());
        }
        this.metricsPort = metricsPort;
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        server = Server.builder()
                .service(
                        "/metrics",
                        (ctx, req) -> HttpResponse.of(registry.scrape()))
                .http(metricsPort)
                .build();
        server.start();
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric metric : metrics) {
            metricChange(metric);
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        LOG.info("Received " + metric.metricName());
        if ((metric.metricValue() instanceof String)) return;
        bindMeter(metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        unbindMeter(metric);
    }

    private void unbindMeter(KafkaMetric metric) {
        registry.remove(meterId(metric));
    }

    @Override
    public void close() {
        server.close();
        registry.close();
    }

    private Id meterId(Metric metric) {
        final String name = meterName(metric);
        final List<Tag> tags = meterTags(metric);
        final Type type;
        if (name.endsWith("total") || name.endsWith("count")) type = Type.COUNTER;
        else type = Type.GAUGE;
        return new Id(name, Tags.of(tags), null, metric.metricName().description(), type);
    }

    private void bindMeter(Metric metric) {
        final Id id = meterId(metric);
        try {
            if (id.getType().equals(Type.COUNTER)) {
                registerCounter(id, metric);
            } else {
                registerGauge(id, metric);
            }
        } catch (Exception ex) {
            String message = ex.getMessage();
            if (message != null && message.contains("Prometheus requires"))
                LOG.info("Failed to bind meter: " + id.getName() + " " + id.getTags()
                        + ". However, this could happen and might be restored in the next refresh.");
            else
                LOG.warn("Failed to bind meter: " + id.getName() + " " + id.getTags() + ".", ex);
        }
    }

    private void registerGauge(Id id, Metric metric) {
        Gauge.builder(id.getName(), metric, toMetricValue())
                .tags(id.getTags())
                .description(metric.metricName().description())
                .register(registry);
    }

    private void registerCounter(Id id, Metric metric) {
        FunctionCounter.builder(id.getName(), metric, toMetricValue())
                .tags(id.getTags())
                .description(metric.metricName().description())
                .register(registry);
    }

    private static ToDoubleFunction<Metric> toMetricValue() {
        return metric -> ((Number) metric.metricValue()).doubleValue();
    }

    private List<Tag> meterTags(Metric metric, boolean includeCommonTags) {
        List<Tag> tags = new ArrayList<>();
        metric.metricName().tags().forEach((key, value) -> tags.add(Tag.of(key, value)));
//        tags.add(Tag.of(KAFKA_VERSION_TAG_NAME, kafkaVersion));
//        extraTags.forEach(tags::add);
        if (includeCommonTags) {
//            commonTags.forEach(tags::add);
        }
        return tags;
    }

    private List<Tag> meterTags(Metric metric) {
        return meterTags(metric, false);
    }

    private String meterName(Metric metric) {
        String name = METRIC_NAME_PREFIX + metric.metricName().group() + "." + metric.metricName().name();
        return name.replaceAll("-metrics", "").replaceAll("-", ".");
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {

    }

    @Override
    public void init(VerifiableProperties props) {
    }
}
