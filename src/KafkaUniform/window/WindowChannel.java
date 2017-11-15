package KafkaUniform.window;

import KafkaUniform.KafkaChannel;

import java.util.Map;

/**
 * Created by Eddie on 2017/11/14.
 */
public class WindowChannel implements KafkaChannel {

    @Override
    public void updateLog(String metricType, Long timestamp, Double value, Map<String, String> tags) {

    }

    @Override
    public void updateMetric(String metricType, Long timestamp, Double value, Map<String, String> tags) {

    }
}
