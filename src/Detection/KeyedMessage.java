package Detection;

import java.util.List;

/**
 * Created by Eddie on 2017/11/9.
 */
public class KeyedMessage {
    String key;
    List<String> identifiers;
    double value;
    MessageType type;

    public enum MessageType {
        PERIOD, INSTANT
    }
}
