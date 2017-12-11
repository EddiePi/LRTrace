package server;

import feedback.FeedbackManager;
import feedback.TestFeedback;
import utils.SystemMetricMonitor;
import docker.DockerMonitorManager;
import log.LogReaderManager;
import kafkaSupport.tsdb.TSManager;

import java.io.IOException;

/**
 * Created by Eddie on 2017/2/21.
 */
public class Tracer {
    private LogReaderManager logReaderManager;

    private DockerMonitorManager dockerMonitorManager;

    private TSManager tsManager;

    private SystemMetricMonitor systemMetricMonitor;

    private FeedbackManager feedbackManager;

    private boolean isTest = false;

    private TestFeedback testFeedback;

    private TracerConf  conf = TracerConf.getInstance();
    private boolean isMaster = conf.getBooleanOrDefault("tracer.is-master", false);
    private boolean systemMonitorEnabled = conf.getBooleanOrDefault("tracer.system-monitor.enabled", false);
    private class TracingRunnable implements Runnable {
        int prevLogReaderNumber = -1;
        int prevDockerMonitorNumber = -1;
        @Override
        public void run() {
            while (true) {
                try {
                    if(logReaderManager.runningContainerMap.size() != prevLogReaderNumber ||
                            dockerMonitorManager.containerIdToDM.size() != prevDockerMonitorNumber) {
                        prevLogReaderNumber = logReaderManager.runningContainerMap.size();
                        prevDockerMonitorNumber = dockerMonitorManager.containerIdToDM.size();
                        System.out.printf("number of LogReader: %d; number of DockerMonitor: %d\n",
                                prevLogReaderNumber, prevDockerMonitorNumber);
                    }
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (NullPointerException e) {
                    // do nothing
                }
            }
        }
    }
    TracingRunnable runnable = new TracingRunnable();

    Thread tThread = new Thread(runnable);

    private static final Tracer instance = new Tracer();
    private Tracer() {

        if (systemMonitorEnabled) {
            systemMetricMonitor = new SystemMetricMonitor();
            systemMetricMonitor.start();
        }
    }

    public static Tracer getInstance() {
        return instance;
    }

    public void init() {
        logReaderManager = new LogReaderManager();
        dockerMonitorManager = new DockerMonitorManager();


        logReaderManager.start();
        tThread.start();
        if(isMaster) {
            tsManager = new TSManager();
            tsManager.start();

            // TEST
            testFeedbackInit();
        }
    }

    /**
     * this class is for test only.
     */
    private void testFeedbackInit() {
        testFeedback = new TestFeedback("test", 5000);
    }

    private void testFeedbackStop() {
        testFeedback.stop();
    }

    /**
     * This method is called by <code>LogReaderManager</code>.
     * Used to create a new <code>DockerMonitor</code>.
     *
     * @param containerId
     */
    public void addContainerMonitor(String containerId) {
        System.out.printf("adding docker monitor for %s\n", containerId);
        dockerMonitorManager.addDockerMonitor(containerId);
    }

    /**
     * This method is called by <code>DockerMonitorManager</code>.
     * Used to stop a <code>LogReaderManager</code>
     *
     * @param containerId
     */
    public void removeContainerLogReader(String containerId) {
        logReaderManager.stopContainerLogReaderById(containerId);
    }

    public void stop() throws IOException {
        logReaderManager.stop();
        dockerMonitorManager.stop();
        if(isMaster) {
            tsManager.stop();
        }
        if (systemMonitorEnabled) {
            systemMetricMonitor.stop();
        }

        // TEST
        testFeedbackStop();
    }
}
