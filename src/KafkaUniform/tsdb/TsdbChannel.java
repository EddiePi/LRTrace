package KafkaUniform.tsdb;

import KafkaUniform.KafkaChannel;
import Server.TracerConf;
import Utils.HTTPRequest;

import java.io.IOException;
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
//        conf = TracerConf.getInstance();
//        databaseURI = conf.getStringOrDefault("tracer.tsdb.server", "localhost:4242");
//        if (!databaseURI.matches("http://.*")) {
//            databaseURI = "http://" + databaseURI;
//        }
//        if (!databaseURI.matches(".*/api/put")) {
//            databaseURI = databaseURI + "/api/put";
//        }
//
//        transferRunnable = new TransferRunnable();
//        transferThread = new Thread(transferRunnable);
//        transferThread.start();
    }

    private class TransferRunnable implements Runnable {
        boolean isRunning = true;

        @Override
        public void run() {
            while (isRunning) {
                if (builder.getMetrics().size() > 0) {
                    try {
                        String message = builder.build(true);
                        // TODO: maintain the connection for performance
                        String response = HTTPRequest.sendPost(databaseURI, message);
                        if (!response.matches("\\s*")) {
                            System.out.printf("Unexpected response: %s\n", response);
                        }
                        Thread.sleep(10);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void updateLog(String key, Long timestamp, Double value, Map<String, String> tags) {
        parseShortContainerId(tags);
        builder.addMetric(key)
                .setDataPoint(timestamp, value)
                .addTags(tags);
    }

    @Override
    public void updateMetric(String metricType, Long timestamp, Double value, Map<String, String> tags) {
        parseShortContainerId(tags);
        builder.addMetric(metricType)
                .setDataPoint(timestamp, value)
                .addTags(tags);
    }

    private void parseShortContainerId(Map<String, String> tags) {
        String longContainerId = tags.get("container");
        if (longContainerId == null) {
            return;
        }
        String[] parts = longContainerId.split("_");
        String shortId = parts[parts.length - 2] + "_" + parts[parts.length - 1];
        tags.put("container", shortId);
    }
}
