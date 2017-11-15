package KafkaUniform.tsdb;

/**
 * Created by Eddie on 2017/6/22.
 */
public class TSManager {
    KafkaToTsdbChannel kafkaToTsdbChannel;
    public TSManager() {
        kafkaToTsdbChannel = new KafkaToTsdbChannel();
    }

    public void start() {
        kafkaToTsdbChannel.start();
    }

    public void stop() {
        kafkaToTsdbChannel.stop();
    }
}
