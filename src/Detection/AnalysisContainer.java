package Detection;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/11/8.
 */
public class AnalysisContainer implements Serializable {
    private String containerId;
    private String appId;
    private Long timestamp;

    // resource metrics;
    public double CPU;
    public double memory;
    public double diskServiceByte;
    public double diskServiceTime;
    public double diskWaitTime;
    public double diskIOTime;
    public double netRate;

    //logs
    public Map<String, List<KeyedMessage>> periodMessages;
    public Map<String, List<KeyedMessage>> instantMessages;

    public AnalysisContainer() {
        periodMessages = new HashMap<>();
        instantMessages = new HashMap<>();
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        if(timestamp.toString().length() > 10) {
            timestamp /= 1000;
        }
        this.timestamp = timestamp;
    }
}
