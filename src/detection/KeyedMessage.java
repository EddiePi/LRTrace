package detection;

import kafkaSupport.MessageType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Eddie on 2017/11/9.
 */
public class KeyedMessage implements Serializable {
    public String key;
    public Map<String, String> identifiers;
    public double value;
    public MessageType type;

    public KeyedMessage() {
        identifiers = new HashMap<>();
    }
}
