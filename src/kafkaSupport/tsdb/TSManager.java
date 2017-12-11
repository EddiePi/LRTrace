package kafkaSupport.tsdb;

import kafkaSupport.KafkaMessageUniform;

/**
 * Created by Eddie on 2017/6/22.
 */
public class TSManager {
    KafkaMessageUniform kafkaMessageUniform;
    public TSManager() {
        kafkaMessageUniform = new KafkaMessageUniform();
    }

    public void start() {
        kafkaMessageUniform.start();
    }

    public void stop() {
        kafkaMessageUniform.stop();
    }
}
