package log;

import Server.TracerConf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by Eddie on 2017/6/8.
 */
public class KafkaLogSender {
    Properties props;
    Producer<String, String> producer;
    TracerConf conf;
    String kafkaTopic;

    // The name is either containerId-log or 'nodemanager-log' or 'resourcemanager-log'.
    String key;

    public KafkaLogSender(String key) {
        conf = TracerConf.getInstance();
        props = new Properties();
        props.put("acks", "0");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String servers = conf.getStringOrDefault("tracer.kafka.bootstrap.servers", "localhost:9092");
        props.put("bootstrap.servers", servers);
        kafkaTopic = "log";
        producer = new KafkaProducer<>(props);

        this.key = key + "-log";

    }

    public void send(String message) {
        producer.send(new ProducerRecord<String, String>(kafkaTopic, key, message));
    }

    public void close() {
        producer.close(10, TimeUnit.SECONDS);
    }
}
