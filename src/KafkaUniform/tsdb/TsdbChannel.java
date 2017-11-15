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

    public TsdbChannel() {
        databaseURI = conf.getStringOrDefault("tracer.tsdb.server", "localhost:4242");
        if (!databaseURI.matches("http://.*")) {
            databaseURI = "http://" + databaseURI;
        }
        if (!databaseURI.matches(".*/api/put")) {
            databaseURI = databaseURI + "/api/put";
        }
    }

    private class TransferRunnalbe implements Runnable {
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
        builder.addMetric(key)
                .setDataPoint(timestamp, value)
                .addTags(tags);
    }

    @Override
    public void updateMetric(String metricType, Long timestamp, Double value, Map<String, String> tags) {

    }
}
