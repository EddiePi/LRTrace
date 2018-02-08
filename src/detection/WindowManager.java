package detection;

import org.python.netty.util.internal.ConcurrentSet;
import server.TracerConf;
import utils.FileIO;
import utils.ObjPersistent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
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

    public Map<String, Map<Long, Map<String, AnalysisContainer>>> appSlidingWindow;

    private Map<String, Integer> appWindowSizeMap;

    private String groupByMethod = conf.getStringOrDefault("tracer.window.group-by", "time");
    private boolean groupByTime;

    private ConcurrentSet<String> appsInFinishState = new ConcurrentSet<>();
    private ConcurrentSet<String> finishedApp = new ConcurrentSet<>();

    private int windowSize;
    private int windowInterval;

    private transient Gson mapper = null;

    private boolean isMSResolution = conf.getBooleanOrDefault("tracer.docker.monitor.ms-resolution", false);

    /**
     * this is used to synchronize the first timestamp index of the running app.
     */
    private Boolean firstData = true;
    private Long currentStartTimestamp = 0L;

    private SelfCheckingRunnable selfCheckingRunnable = new SelfCheckingRunnable();
    private Thread selfCheckingThread = new Thread(selfCheckingRunnable);

    // one of 'storage', 'training', 'detection'
    private String mode = conf.getStringOrDefault("tracer.detection.mode", "detection");
    private String storagePath = conf.getStringOrDefault("tracer.detection.data-path", "./data");

    private String dataFilePrefix = "sliding_window";

    private class SelfCheckingRunnable implements Runnable {

        boolean isChecking = true;
        AtomicInteger idleCount = new AtomicInteger(0);

        @Override
        public void run() {
            //System.out.printf("self checking thread started.");
            while(isChecking) {
                if (groupByTime) {
                    //System.out.printf("idle count: %d\n", idleCount.get());
                    if ((mode.equals("detection") && !hasMoreData() || mode.equals("storage")) &&
                            idleCount.get() < 3) {
                        idleCount.incrementAndGet();
                    }
                    if (idleCount.get() >= 3 && !firstData) {
                        maybeStoreWindow();
                        firstData = true;
                    }
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (mode.equals("storage")) {
                        if (!appsInFinishState.isEmpty()) {
                            maybeStoreAppAsJson();
                        }
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        private void maybeStoreWindow() {
            // TEST
            //System.out.printf("maybe store window. Window size: %d\n", slidingWindow.size());
            if (slidingWindow.size() != 0) {
                storeSlidingWindow();
                storeSlidingWindowAsJson();
                slidingWindow.clear();
            }
        }

        public void resetCount() {
            idleCount.set(0);
        }
    }

    private WindowManager() {
        windowSize = conf.getIntegerOrDefault("tracer.window.size", 1000);
        windowInterval = conf.getIntegerOrDefault("tracer.window.interval", windowSize);
        if (groupByMethod.toLowerCase().equals("app")) {
            groupByTime = false;
        } else {
            groupByTime = true;
        }
        if (!isMSResolution && windowSize < 1000) {
            windowInterval = 1000;
            windowSize = 1000;
        }
        if (windowInterval > windowSize) {
            windowInterval = windowSize;
        }
        if (groupByTime) {
            slidingWindow = new ConcurrentSkipListMap<>();
        } else {
            appSlidingWindow = new ConcurrentSkipListMap<>();
            appWindowSizeMap = new ConcurrentHashMap<>();
        }

        selfCheckingThread.start();
        if (mode.equals("training")) {
            loadSlidingWindow();
        }
        System.out.printf("window manager started. window size: %d\n", windowSize);
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
        if (timestamp.toString().length() > 10) {
            timestamp /= windowSize;
            timestamp *= windowSize;
        }
        if (groupByTime) {

            if (firstData && !hasMoreData()) {
                synchronized (this.firstData) {
                    currentStartTimestamp = timestamp;
                    firstData = false;
                }
            }
            selfCheckingRunnable.resetCount();

            // TEST
            // System.out.printf("updating container: %s, time: %d\n", containerId, timestamp);

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
        } else {
            String appId;
            if (containerId.matches("app.*")) {
                appId = containerId;
            } else {
                appId = containerToAppId(containerId);
            }
            if (!appId.matches("app.*")) {
                System.out.printf("invalid app id. original container id: %s\n", containerId);
            }
            Map<Long, Map<String, AnalysisContainer>> appMap = appSlidingWindow.get(appId);
            if (appMap == null) {
                appMap = new LinkedHashMap<>();
                appSlidingWindow.put(appId, appMap);
            }
            Map<String, AnalysisContainer> containerMap = appMap.get(timestamp);
            if (containerMap == null) {
                containerMap = new HashMap<>();
                appMap.put(timestamp, containerMap);
            }
            AnalysisContainer container = containerMap.get(containerId);
            if (container == null) {
                container = new AnalysisContainer();
                containerMap.put(containerId, new AnalysisContainer());
            }
            assert(container != null);
            return container;
        }
    }


    /**
     * this function returns a <code>list</code> of container in the current window.
     * this. data in one window is raw. function is used to analysis.
     * @return
     */
    public List<Map<String, AnalysisContainer>> getWindowedDataForAnalysis(Long timestampUpperBound) {
        if (timestampUpperBound.toString().length() > 10) {
            timestampUpperBound /= windowSize;
            timestampUpperBound *= windowSize;
        }
        List<Map<String, AnalysisContainer>> dataForAnalysis = new ArrayList<>();
        int size = Math.min(windowSize, slidingWindow.size());
        for(int i = 0; i < size; i++) {
            Map<String, AnalysisContainer> containerMap;

            if (currentStartTimestamp + i > timestampUpperBound) {
                break;
            }

            // delete the data in the window when the current index is less than the interval.
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

    public void registerFinishedApp(String appId) {
        if (!finishedApp.contains(appId)) {
            synchronized (this.appsInFinishState) {
                appsInFinishState.add(appId);
                appWindowSizeMap.put(appId, 0);
            }
            finishedApp.add(appId);
        }
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

    public void storeSlidingWindowAsJson() {
        File dataPath = new File(storagePath);
        String[] files = dataPath.list();
        Integer existingDataFiles = 0;
        for (String file : files) {
            if (file.matches(dataFilePrefix + "[0-9]+" + "\\.json")) {
                existingDataFiles++;
            }
        }
        String fullPath = storagePath + "/" + dataFilePrefix + existingDataFiles.toString() + ".json";

        if (mapper == null) {
            GsonBuilder builder = new GsonBuilder();
            mapper = builder.create();
        }
        String resultJson = mapper.toJson(slidingWindow);
        try {
            FileIO.write(fullPath, resultJson);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void maybeStoreAppAsJson() {
        Set<String> appToStore = new HashSet<>();
        for (Map.Entry<String, Integer> entry: appWindowSizeMap.entrySet()) {
            String appId = entry.getKey();
            int previousSize = entry.getValue();
            int currentSize = appSlidingWindow.get(appId).size();
            if (previousSize == currentSize) {
                System.out.printf("new app to store: %s\n", appId);
                appToStore.add(appId);
            } else {
                entry.setValue(currentSize);
            }
        }
        for (String appToRemove: appToStore) {
            appsInFinishState.remove(appToRemove);
            appWindowSizeMap.remove(appToRemove);
        }
        Map<Long, Map<String, AnalysisContainer>> oneAppWindow;
        if (mapper == null) {
            GsonBuilder builder = new GsonBuilder();
            mapper = builder.create();
        }
        for (String appId: appToStore) {
            oneAppWindow = new LinkedHashMap<>(appSlidingWindow.get(appId));
            String resultJson = mapper.toJson(oneAppWindow);
            String fullPath = storagePath + "/" + appId + ".json";
            try {
                FileIO.write(fullPath, resultJson);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String containerToAppId(String containerId) {
        String res = null;
        if (containerId.matches("container.*")) {
            String[] parts = containerId.split("_");
            res = "application" + "_" + parts[1] + "_" + parts[2];
        }
        return res;
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

        slidingWindow.isEmpty();
    }

    public boolean hasMoreData() {
        return slidingWindow.size() != 0;
    }
}
