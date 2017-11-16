package Detection;

import Server.TracerConf;
import Utils.ObjPersistent;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by Eddie on 2017/11/9.
 */
public class WindowManager {

    private static WindowManager instance = null;

    public static WindowManager getInstance() {
        if (instance == null) {
            instance = new WindowManager();
        }
        return instance;
    }

    private TracerConf conf = TracerConf.getInstance();

    //public for test
    public Map<Long, Map<String, AnalysisContainer>> slidingWindow;

    private int windowSize;
    private int windowInterval;

    private Boolean firstData = true;
    private Long currentStartTimestamp;

    private SelfCheckingRunnable selfCheckingRunnable = new SelfCheckingRunnable();
    private Thread selfCheckingThread = new Thread(selfCheckingRunnable);

    // one of 'storage', 'training', 'detection'
    private String mode = conf.getStringOrDefault("tracer.detection.mode", "detection");
    private String storagePath = conf.getStringOrDefault("tracer.detection.data-path", "./data");
    private String dataFilePrefix = "sliding_window";

    private class SelfCheckingRunnable implements Runnable {

        boolean isChecking = true;
        int idleCount = 0;

        @Override
        public void run() {
            while(isChecking) {
                if(!hasMoreData() && idleCount < 6) {
                    idleCount++;
                }
                if (idleCount >= 6 && !firstData) {

                    firstData = true;
                }
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void resetCount() {
            idleCount = 0;
        }
    }

    private WindowManager() {
    }

    public void instantiateWindow(int size, int interval) {

        windowSize = size;
        windowInterval = interval;
        if (windowInterval > windowSize) {
            windowInterval = windowSize;
        }
        slidingWindow = new ConcurrentSkipListMap<>();

        selfCheckingThread.start();
        if (mode.equals("training")) {
            loadSlidingWindow();
        }
    }


    /**
     * this function returns a <code>list</code> of container in the current window.
     * this. function is used to analysis.
     * @return
     */
    public List<Map<String, AnalysisContainer>> getWindowedDataForAnalysis() {
        List<Map<String, AnalysisContainer>> dataForAnalysis = new ArrayList<>();
        int size = Math.min(windowSize, slidingWindow.size());
        for(int i = 0; i < size; i++) {
            Map<String, AnalysisContainer> containerMap;

            // delete when the current index is less than the interval.
            // keep the data otherwise.
            if (i < windowInterval) {
                containerMap = slidingWindow.remove(currentStartTimestamp + i);
            } else {
                containerMap = slidingWindow.get(currentStartTimestamp + i);
            }
            if (containerMap == null) {
                if (dataForAnalysis.size() != 0) {
                    currentStartTimestamp += Math.min(windowInterval, dataForAnalysis.size());
                    return dataForAnalysis;
                } else if (hasMoreData()) {
                    while (slidingWindow.get(currentStartTimestamp) == null) {
                        currentStartTimestamp++;
                    }
                    i = -1;
                    continue;
                }
            } else {
                dataForAnalysis.add(containerMap);
            }
        }
        currentStartTimestamp += Math.min(windowInterval, dataForAnalysis.size());

        return dataForAnalysis;
    }

    /**
     * this method returns the <code>AnalysisContainer</code>.
     * other class will assign this container.
     * if the requested container is not in the sliding window, we create it.
     * @param timestamp
     * @param containerId
     * @return
     */
    public AnalysisContainer getContainerToAssign(Long timestamp, String containerId) {


        if (firstData && !hasMoreData()) {
            synchronized (this.firstData) {
                currentStartTimestamp = timestamp;
                firstData = false;
            }
            selfCheckingRunnable.resetCount();
        }

        // If the incoming data is out of date, we do not create it in the sliding window.
        if (timestamp < currentStartTimestamp) {
            return null;
        }

        Map<String, AnalysisContainer> timestampMap = slidingWindow.get(timestamp);
        if (timestampMap == null) {
            timestampMap = new HashMap<>();
            slidingWindow.put(timestamp, timestampMap);
        }
        assert (timestampMap != null);
        AnalysisContainer container = timestampMap.get(containerId);
        if (container == null) {
            container = new AnalysisContainer();
            container.setTimestamp(timestamp);
            container.setContainerId(containerId);
            timestampMap.put(containerId, container);
        }

        return container;
    }


    /**
     * this should be called by other class
     */
    public void storeSlidingWindow() {
        File dataPath = new File(storagePath);
        String[] files = dataPath.list();
        Integer existingDataFiles = 0;
        for (String file: files) {
            if (file.matches(dataFilePrefix + "[0-9]+")) {
                existingDataFiles++;
            }
        }
        ObjPersistent.saveObject(slidingWindow, storagePath + "/" + dataFilePrefix + existingDataFiles.toString());
    }

    public void loadSlidingWindow() {
        File dataPath = new File(storagePath);
        String[] files = dataPath.list();
        Integer fileIndex = 0;
        Map<Long, Map<String, AnalysisContainer>> readMap;
        for (String file: files) {
            if (file.matches(dataFilePrefix + "[0-9]+")) {
                readMap = (Map<Long, Map<String,AnalysisContainer>>) ObjPersistent.readObject(storagePath + "/" + dataFilePrefix + fileIndex.toString());
                this.slidingWindow.putAll(readMap);
                fileIndex++;
            }
        }
    }

    public boolean hasMoreData() {
        return slidingWindow.size() != 0;
    }
}
