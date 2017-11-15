package Detection;

import java.util.List;

/**
 * Created by Eddie on 2017/11/8.
 */
public class AnalysisContainer {
    private String containerId;
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
    public List<KeyedMessage> periodMessages;
    public List<KeyedMessage> instantMessages;

    public String getContainerId() {
        return containerId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
