package docker;

import Server.Tracer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Eddie on 2017/6/11.
 */
public class DockerMonitorManager {
    // TODO: use thread pool
    public ConcurrentMap<String, DockerMonitor> containerIdToDM = new ConcurrentHashMap<>();
    private Tracer tracer = Tracer.getInstance();

    public DockerMonitorManager () {}

    public void addDockerMonitor(String containerId) {
        if (!containerIdToDM.containsKey(containerId)) {
            DockerMonitor dockerMonitor = new DockerMonitor(containerId, this);
            dockerMonitor.start();
            containerIdToDM.put(containerId, dockerMonitor);
        }
    }

    /**
     * called by DockerMonitor to stop itself.
     *
     * @param containerId
     */
    public void removeDockerMonitor(String containerId) {
        containerIdToDM.remove(containerId);

        // Notify the tracer to remove LogReader of this container.
        tracer.removeContainerLogReader(containerId);
    }

    public void stop() {
        for(DockerMonitor dockerMonitor: containerIdToDM.values()) {
            dockerMonitor.stop();
        }
    }
}
