package docker;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/2/23.
 */
public class DockerMetrics {
    public String dockerId;
    public String containerId;
    // docker taskMetrics
    // unit: second
    public Long timestamp;

    public String state;

    /**
     * @deprecated we no longer record log event with metrics
     */
    @Deprecated
    public List<String> eventList;

    // cpu usage
    public Long sysCpuTime = 0L;
    public Long dockerCpuTime = 0L;
    public Double cpuRate = 0.0;

    // memory usage
    public Long memoryLimit = 0L;
    public Long memoryUsage = 0L;

    // disk taskMetrics
    public Long diskServiceBytes = 0L;
    public Long diskServiceTime = 0L;
    public Long diskQueued = 0L;
    public Long diskIOTime = 0L;
    public Long diskWaitTime = 0L;
    public Double diskRate = 0.0;

    // network taskMetrics
    public Long netRecBytes = 0L;
    public Long netTransBytes = 0L;
    public Double netRecRate = 0.0;
    public Double netTransRate = 0.0;

    public DockerMetrics(String dockerId, String containerId) {
        timestamp = System.currentTimeMillis();
        this.dockerId = dockerId;
        this.containerId = containerId;
        //this.eventList = new ArrayList<>();
    }
}
