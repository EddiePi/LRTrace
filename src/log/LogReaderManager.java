package log;

import Server.Tracer;
import Server.TracerConf;
import Utils.FileWatcher.FileActionCallback;
import Utils.FileWatcher.WatchDir;
import logAPI.*;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Eddie on 2017/6/6.
 */
public class LogReaderManager {
    TracerConf conf;
    File appRootDir;
    File[] applicationDirs;
    File nodeManagerLog;
    File resourceManagerLog;
    File testLog;
    Tracer tracer = Tracer.getInstance();
    // Key is the container's id.
    public ConcurrentMap<String, ContainerLogReader> runningContainerMap = new ConcurrentHashMap<>();
    LogReaderRunnable nodeManagerReaderRunnable;
    LogReaderRunnable resourceManagerReaderRunnable;
    LogReaderRunnable testReaderRunnable;
    CheckAppDirRunnable checkingRunnable;
    Thread checkingThread;
    Thread nodeManagerReadThread;
    Thread resourceManagerReadThread;
    Thread testReaderThread;
    ContainerStateRecorder recorder = ContainerStateRecorder.getInstance();
    LogAPICollector apiCollector;
    Map<String, Integer> newAppList;
    boolean customAPIEnabled;
    boolean isMaster;
    List<MessageMark> managerRules;

    boolean testMode;

    public LogReaderManager() {
        conf = TracerConf.getInstance();
        isMaster = conf.getBooleanOrDefault("tracer.is-master", false);
        managerRules = LogAPICollector.getInstance().managerRuleMarkList;
        appRootDir = new File(conf.getStringOrDefault("tracer.log.app.root", "~/hadoop-2.7.3/logs/userlogs"));
        testMode = conf.getBooleanOrDefault("tracer.log.testmode.enabled", false);
        applicationDirs = appRootDir.listFiles();
        newAppList = new HashMap<>();
        if (applicationDirs == null) {
            applicationDirs = new File[0];
        }
        String username = System.getProperty("user.name");
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        String yarnLogRootDir = conf.getStringOrDefault("tracer.log.nodemanager.root", "~/hadoop-2.7.3/logs");
        nodeManagerLog = new File(yarnLogRootDir + "/yarn-" + username + "-nodemanager-" + hostname + ".log");
        if(!nodeManagerLog.exists()) {
            nodeManagerLog = new File(yarnLogRootDir + "/hadoop-" + username + "-nodemanager-" + hostname + ".log");
        }

        nodeManagerReaderRunnable = new LogReaderRunnable("nodemanager", nodeManagerLog);
        checkingRunnable = new CheckAppDirRunnable();
        nodeManagerReadThread = new Thread(nodeManagerReaderRunnable);
        checkingThread = new Thread(checkingRunnable);

        apiCollector = LogAPICollector.getInstance();
        // if the trace server runs with resource manager, we need to monitor the log of RM, and send log to Opentsdb.
        if (isMaster) {
            resourceManagerLog = new File(yarnLogRootDir + "/yarn-" + username + "-resourcemanager-" + hostname + ".log");
            if(!resourceManagerLog.exists()) {
                resourceManagerLog = new File(yarnLogRootDir + "/hadoop-" + username + "-resourcemanager-" + hostname + ".log");
            }
            resourceManagerReaderRunnable = new LogReaderRunnable("resourcemanager", resourceManagerLog);
            resourceManagerReadThread = new Thread(resourceManagerReaderRunnable);
        }
        registerDefaultAPI();
        customAPIEnabled = conf.getBooleanOrDefault("tracer.log.custom-api.enabled", false);
        if (customAPIEnabled) {
            registerCustomAPI();
        }

        if (!isMaster && testMode) {
            testLog = new File(conf.getStringOrDefault("tracer.log.testmode.dir", "./"));
            if(testLog.exists()) {
                testReaderRunnable = new LogReaderRunnable("testlog", testLog);
                testReaderThread = new Thread(testReaderRunnable);
            }

        }
    }

    private class LogReaderRunnable implements Runnable {
        File managerLogFile;
        KafkaLogSender logSender;
        String kafkaKey;
        boolean isReading;
        List<String> messageBuffer;
        BufferedReader bufferedReader;
        // when we start the tracing server, we need to navigate to the end of the file.
        // when the variable is true, we do not send what we read to kafka.
        boolean isNavigating;
        LogReaderRunnable(String kafkaKey, File managerLogFile) {
            this.kafkaKey = kafkaKey;
            this.managerLogFile = managerLogFile;
            logSender = new KafkaLogSender(kafkaKey);
            isReading = true;
            messageBuffer = new ArrayList<>();
            bufferedReader = null;
            isNavigating = true;
        }

        @Override
        public void run() {
            System.out.printf("start %s log reader.\n", kafkaKey);
            while(isReading) {
                try {
                    if (!managerLogFile.exists()) {
                        Thread.sleep(1000);
                        isNavigating = false;
                        continue;
                    }
                    String line;
                    messageBuffer.clear();
                    if (bufferedReader == null) {
                        InputStream is = new FileInputStream(managerLogFile);
                        Reader reader = new InputStreamReader(is, "GBK");
                        bufferedReader = new BufferedReader(reader);
                    }
                    if(isNavigating) {
                        while(bufferedReader.readLine() != null);
                        isNavigating = false;
                    }
                    while ((line = bufferedReader.readLine()) != null) {
                        if (!kafkaKey.equals("testlog")) {
                            for (MessageMark mm : managerRules) {
                                Pattern pattern = Pattern.compile(mm.regex);
                                Matcher matcher = pattern.matcher(line);
                                if (matcher.matches()) {
                                    messageBuffer.add(line);
                                    break;
                                }
                            }
                        } else {
                            messageBuffer.add(line);
                        }
                    }
                    for (String message : messageBuffer) {
                        logSender.send(message);
                    }
                    Thread.sleep(200);

                } catch (InterruptedException e) {

                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void destroy() throws IOException {
            isReading = false;
            logSender.close();
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
    }

    private class CheckAppDirRunnable implements Runnable {
        WatchDir watchDir;
        @Override
        public void run() {
            System.out.print("app checking thread started.\n");
            try {
                watchDir = new WatchDir(appRootDir, true, new FileActionCallback() {
                    @Override
                    public void create(File file) {
                        System.out.println("file created\t" + file.getAbsolutePath());

                        // The file name is also the containerId.
                        String name = file.getName();
                        if(name.contains("application")) {
                            // Test
                            System.out.printf("new app: %s\n", name);
                            newAppList.put(name, 1);
                        }
                        if(name.contains("container")) {
                            String appId = containerIdToAppId(name);
                            if(newAppList.get(appId) != null) {
                                tracer.addContainerMonitor(name);
                                runningContainerMap.put(name, new ContainerLogReader(file.getAbsolutePath()));
                            }

                        }
                    }

                    @Override
                    public void delete(File file) {}

                    @Override
                    public void modify(File file) {}
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        checkingThread.start();
        nodeManagerReadThread.start();
        if (isMaster) {
            resourceManagerReadThread.start();
        }
        if (!isMaster && testMode) {
            testReaderThread.start();
        }
    }

    public void stopContainerLogReaderById(String containerId) {
        ContainerLogReader logReaderToRemove =
                runningContainerMap.remove(containerId);
        if(logReaderToRemove != null) {
            logReaderToRemove.stop();
        }
    }

    public void stop() throws IOException {
        nodeManagerReaderRunnable.destroy();
        for(ContainerLogReader logReader: runningContainerMap.values()) {
            logReader.stop();
        }
        if(isMaster) {
            resourceManagerReaderRunnable.destroy();
        }
        if (!isMaster && testMode) {
            testReaderRunnable.destroy();
        }
    }

    /**
     * @deprecated this method is deprecated. we should do this function on master node
     * @param logStr
     */
    @Deprecated
    private void recordContainerState(String logStr) {
        if(logStr.matches(".*Start request for container.*")) {

            // here we notify the docker monitor to start.
            String[] words = logStr.split("\\s+");
            Long timestamp = parseTimestamp(logStr);
            String firstState = "NEW";
            String containerId = words[words.length - 4];
            System.out.printf("filtered message: %s\ncontainerId: %s, state: %s\n", logStr, containerId, firstState);
            recorder.putState(containerId, firstState, timestamp);

            // start docker monitor
        } else if(logStr.matches(".*Container.*transitioned from.*")) {
            Long timestamp = parseTimestamp(logStr);
            String[] words = logStr.split("\\s+");
            String nextState = words[words.length - 1];
            String containerId = words[words.length - 6];
            System.out.printf("filtered message: %s\ncontainerId: %s, state: %s\n", logStr, containerId, nextState);
            recorder.putState(containerId, nextState, timestamp);
        }
    }


    // we need to re-registerContainerRules the API after detecting new apps.
    // In this design, we don't have to restart the tracing server to import changed api file
    private void registerDefaultAPI() {
        List<AbstractLogAPI> defaultContainerAPIList = new ArrayList<>();
        defaultContainerAPIList.add(new SparkLogAPI());
        defaultContainerAPIList.add(new MapReduceLogAPI());

        for(AbstractLogAPI api: defaultContainerAPIList) {
            apiCollector.registerContainerRules(api);
        }
        apiCollector.registerManagerRules(new YarnLogAPI());
    }

    private void registerCustomAPI() {
        String[] filePaths = conf.getStringOrDefault("tracer.log.custom-api.path", "../conf/custom.api").split(",");
        for(String filePath: filePaths) {
            File apiFile = new File(filePath);
            if(apiFile.exists()) {
                apiCollector.registerContainerRules(new CustomLogAPI(apiFile));
            }
        }
    }

    public static Long parseTimestamp(String logMessage) {
        String[] words = logMessage.split("\\s+");
        Long timestamp = Timestamp.valueOf(words[0].replace('/', '-') + " " + words[1].replace(',', '.')).getTime();
        return timestamp;
    }

    private String containerIdToAppId(String containerId) {
        String[] parts = containerId.split("_");
        String appId = "application_" + parts[parts.length - 4] + "_" + parts[parts.length - 3];
        return appId;
    }
}
