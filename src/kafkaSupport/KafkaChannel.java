package kafkaSupport;

import java.util.Map;

/**
 * Created by Eddie on 2017/11/14.
 */
public interface KafkaChannel {

    void updateLog(String key, Long timestamp, Double value, Map<String, String> tags);

    void updateMetric(String metricType, Long timestamp, Double value, Map<String, String> tags);
}
