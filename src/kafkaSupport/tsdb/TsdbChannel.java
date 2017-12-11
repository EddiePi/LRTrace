package kafkaSupport.tsdb;

import kafkaSupport.KafkaChannel;
import server.TracerConf;
import utils.HTTPRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Eddie on 2017/11/14.
 */
public class TsdbChannel implements KafkaChannel {

    TsdbMetricBuilder builder = TsdbMetricBuilder.getInstance();
    String databaseURI;
    TracerConf conf;

    TransferRunnable transferRunnable;
    Thread transferThread;

    public TsdbChannel() {
        conf = TracerConf.getInstance();
        databaseURI = conf.getStringOrDefault("tracer.tsdb.server", "localhost:4242");
        if (!databaseURI.matches("http://.*")) {
            databaseURI = "http://" + databaseURI;
        }
        if (!databaseURI.matches(".*/api/put")) {
            databaseURI = databaseURI + "/api/put";
        }

        transferRunnable = new TransferRunnable();
        transferThread = new Thread(transferRunnable);
        transferThread.start();
        System.out.print("TSDB channel started\n");
    }

    private class TransferRunnable implements Runnable {
        boolean isRunning = true;

        @Override
        public void run() {
            while (isRunning) {
                if (builder.getMetrics().size() > 0) {
                    try {
                        String message;
                        synchronized (builder) {
                            message = builder.build(true);
                        }
                        // TODO: maintain the connection for performance
                        String response = HTTPRequest.sendPost(databaseURI, message);
                        if (!response.matches("\\s*")) {
                            System.out.printf("Unexpected response: %s\n", response);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * the key has it type, either <code>instant</code> or <code>period</code>
     * we need to take off the type before send it to opentsdb.
     * @param key
     * @param timestamp
     * @param value
     * @param tags
     */
    @Override
    public void updateLog(String key, Long timestamp, Double value, Map<String, String> tags) {
        Map<String, String> tsdbTags;
        tsdbTags = parseShortContainerId(tags);
        String realKey;
        String[] typeKey = key.split(":");
        if (typeKey.length >= 2) {
            realKey = typeKey[1];
        } else {
            realKey = key;
        }
        synchronized (builder) {
            builder.addMetric(realKey)
                    .setDataPoint(timestamp, value)
                    .addTags(tsdbTags);
        }
    }

    @Override
    public void updateMetric(String metricType, Long timestamp, Double value, Map<String, String> tags) {
        Map<String, String> tsdbTags;
        tsdbTags = parseShortContainerId(tags);
        synchronized (builder) {
            builder.addMetric(metricType)
                    .setDataPoint(timestamp, value)
                    .addTags(tsdbTags);
        }
    }

    private Map<String, String> parseShortContainerId(Map<String, String> tags) {
        Map<String, String> newTags = new HashMap<>(tags);
        String longContainerId = tags.get("container");
        if (longContainerId == null) {
             return newTags;
        }
        String[] parts = longContainerId.split("_");
        String shortId = parts[parts.length - 2] + "_" + parts[parts.length - 1];
        newTags.put("container", shortId);

        return newTags;
    }
}
