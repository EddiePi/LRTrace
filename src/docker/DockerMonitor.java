package docker;

import Server.TracerConf;
import Utils.ShellCommandExecutor;
import log.AppObjRecorder;
import log.ContainerStateRecorder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/2/15.
 */
class DockerMonitor {
    TracerConf conf = TracerConf.getInstance();
    private String dockerId;
    private DockerMonitorManager manager;
    String containerId;

    // NOTE: type of dockerPid is String, NOT int
    String dockerPid = null;
    private String blkioPath;
    private String netFilePath;
    private String cpuPath;
    private String memoryPath;
    private MonitorRunnable monitorRunnable;
    private Thread monitorThread;

    private String ifaceName;
    private DockerMetrics previousMetrics = null;
    private DockerMetrics currentMetrics = null;

    private KafkaMetricSender metricSender = new KafkaMetricSender();

    volatile private boolean isRunning;

    private boolean isRealDockerOn = false;
    private long monitorInterval;

    private long netTransOffset = 0L;
    private long netRecOffset = 0L;

    public DockerMonitor(String containerId, DockerMonitorManager dmManger) {
        this.containerId = containerId;
        this.manager = dmManger;

        ifaceName  = conf.getStringOrDefault("tracer.docker.iface-name", "eth0");
        if (conf.getBooleanOrDefault("tracer.docker.ms-resolution", false)) {
            monitorInterval = conf.getIntegerOrDefault("tracer.docker.monitor-interval", 50);
        } else {
            monitorInterval = 1000;
        }

        monitorRunnable = new MonitorRunnable();
        monitorThread = new Thread(monitorRunnable);
    }

    public void start() {
        isRunning = true;
        monitorThread.start();
    }

    public void stop() {
        monitorRunnable.stop();
    }

    public String getDockerId() {
        return dockerId;
    }

    public String getDockerPid() {
        return dockerPid;
    }

    // Run a given shell command. return a string as the result
    private String runShellCommand(String command){

        ShellCommandExecutor shExec = null;
        int count = 1;
        while(count < 110){
            //we try 10 times if fails due to device busy
            try {
                shExec = new ShellCommandExecutor(command);
                shExec.execute();

            } catch (IOException e) {
                count++;
                try {
                    Thread.sleep(100 * count);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                continue;

            }
            break;
        }
        return shExec.getOutput().trim();
    }

    private class MonitorRunnable implements Runnable {

        @Override
        public void run(){
//            try {
//                Thread.sleep(1100);
//            } catch (InterruptedException e) {
//                // do nothing
//            }
            //int count = 3;
            //int index = 0;
            int maxRetry = 0;
            while (isRunning) {
                if(!isRealDockerOn) {
                    dockerId = runShellCommand("docker inspect --format={{.Id}} " + containerId);
                    if(dockerId.contains("Error") || dockerId.length() == 0) {
                        sendZeroMetrics();
                        maxRetry++;
                        if (maxRetry >= 10) {
                            System.out.print("no docker started after 10 retry. abandon monitoring the docker\n");
                            isRunning = false;
                            manager.removeDockerMonitor(containerId);
                            break;
                        }
                        System.out.printf("docker for %s is not started yet. retry in %d milliseconds.\n",
                                containerId, monitorInterval);
                    } else {
                        dockerPid = runShellCommand("docker inspect --format={{.State.Pid}} " + containerId).trim();
                        cpuPath = "/sys/fs/cgroup/cpu,cpuacct/docker/" + dockerId + "/";
                        memoryPath = "/sys/fs/cgroup/memory/docker/" + dockerId + "/";
                        blkioPath= "/sys/fs/cgroup/blkio/docker/" + dockerId + "/";
                        netFilePath = "/proc/" + dockerPid + "/net/dev";
                        isRealDockerOn = true;
                        System.out.print(String.format("docker id: %s\n", dockerId));
                    }
                } else {
                    // monitor the docker info
                    updateCgroupValues();
                    // printStatus();
                }
                //if we come here it means we need to sleep for monitorInterval milliseconds
                try {
                    Thread.sleep(monitorInterval);
                } catch (InterruptedException e) {
                    //do nothing
                }
            }
        }

        public void stop() {
            isRunning = false;
        }
    }

    private void sendZeroMetrics() {
        DockerMetrics zeroMetrics = new DockerMetrics(null, containerId);
        // getState(zeroMetrics);
        metricSender.send(zeroMetrics);
    }

    public void updateCgroupValues() {
        previousMetrics = currentMetrics;
        currentMetrics = new DockerMetrics(dockerId, containerId);

        // get the container's state. e.g INIT, LOCALIZING ...
        // getState(currentMetrics);

        // get the app information in the container
        // getEvent(currentMetrics);

        // calculate the cpu rate
        calculateCurrentCpuRate(currentMetrics);

        // get memory usage
        getMemoryUsage(currentMetrics);

        // calculate the disk rate
        calculateCurrentDiskRate(currentMetrics);

        // calculate the network rate
        calculateCurrentNetMetric(currentMetrics);

        metricSender.send(currentMetrics);
        if(!isRunning) {
            metricSender.close();
        }

        // TEST
        // printStatus();
    }

    /**
     * @deprecated we no longer record state info with metric.
     * @param m
     */
    @Deprecated
    private void getState(DockerMetrics m) {
        ContainerStateRecorder stateRecorder = ContainerStateRecorder.getInstance();
        m.state = stateRecorder.getState(m.containerId, m.timestamp, true);
    }

    /**
     *
     * @deprecated we do not record container state in metric info anymore.
     * Instead, we record it in log file
     * @param m
     */
    @Deprecated
    private void getEvent(DockerMetrics m) {
        AppObjRecorder objRecorder = AppObjRecorder.getInstance();
        List<String> temp = objRecorder.getInfo(m.containerId, m.timestamp);
        if (temp != null) {
            if (m.eventList == null) {
                m.eventList = new ArrayList<>();
            }
            m.eventList.addAll(temp);
            System.out.print("current app event: ");
            for(String event: m.eventList){
                System.out.print(event + "\t");
            }
            System.out.print("\n");
        }
    }

    private boolean getCpuTime(DockerMetrics m) {
        if(!isRunning) {
            return false;
        }
        boolean calRate = true;
        if(previousMetrics == null) {
            calRate = false;
        }

        String dockerUrl = cpuPath + "cpuacct.usage";
        String sysUrl = "/proc/stat";

        // parse the docker's cpu time
        List<String> readLines = readFileLines(dockerUrl);
        if(readLines != null) {
            String dockerCpuTimeStr = readLines.get(0);
            m.dockerCpuTime = Long.parseLong(dockerCpuTimeStr) / 1000000;
        }
        readLines = readFileLines(sysUrl);
        if(readLines != null) {
            String[] firstLineStr = readLines.get(0).split("\\s+");
            Long sysCpuTime = 0L;
            for(int i = 1; i < firstLineStr.length; i++) {
                sysCpuTime += Long.parseLong(firstLineStr[i]);
            }
            m.sysCpuTime = sysCpuTime;
        }
        return calRate;
    }

    private void calculateCurrentCpuRate(DockerMetrics m) {
        if(!getCpuTime((m))) {
            return;
        }

        //calculate the rate
        Double deltaSysTime = (m.sysCpuTime - previousMetrics.sysCpuTime) * 1.0;
        Double deltaDockerTime = (m.dockerCpuTime - previousMetrics.dockerCpuTime) * 1.0;
        m.cpuRate = Math.max(0.0D, deltaDockerTime / deltaSysTime);
    }

    private void getMemoryUsage(DockerMetrics m) {
        if(!isRunning) {
            return;
        }
        // get the memory limit
        String sysMaxMemoryStr = readFileLines("/proc/meminfo").get(0).split("\\s+")[1];
        Long sysMaxMemory = Long.parseLong(sysMaxMemoryStr);
        String dockerMaxMemoryStr = readFileLines(memoryPath + "memory.limit_in_bytes").get(0);
        Long dockerMaxMemory = Long.parseLong(dockerMaxMemoryStr) / 1000;
        m.memoryLimit = Math.min(sysMaxMemory, dockerMaxMemory);

        String memoryUsageStr = readFileLines(memoryPath + "memory.usage_in_bytes").get(0);
        Long memoryUsage = Long.parseLong(memoryUsageStr);
        m.memoryUsage = memoryUsage;
    }

    // calculate the disk I/O rate(kb/s)
    private void calculateCurrentDiskRate(DockerMetrics m) {
        if (!getDiskServicedBytes(m)) {
            return;
        }
        // init timestamps
        Double deltaTime = (m.timestamp - previousMetrics.timestamp) / 1000.0;

        // calculate the rate
        Long deltaBytes = m.diskServiceBytes - previousMetrics.diskServiceBytes;
        m.diskRate = deltaBytes / deltaTime/ 1000d;
        //System.out.print("deltaTime: " + deltaTime + " deltaRead: " + deltaRead + " deltaWrite: " + deltaWrite + "\n");
    }

    // parse the disk usages from cgroup files
    // and update the taskMetrics in the monitor.
    // if it is not running or first parse, return false.
    private boolean getDiskServicedBytes(DockerMetrics m) {
        if(!isRunning) {
            return false;
        }
        boolean calRate = false;


        String serviceBytesStr = readDiskFileFormat(blkioPath + "blkio.io_service_bytes");
        if (serviceBytesStr != null) {
            m.diskServiceBytes = Long.parseLong(serviceBytesStr);
            if(previousMetrics != null) {
                calRate = true;
            }
        }

        String waitTimeStr = readDiskFileFormat(blkioPath + "blkio.io_wait_time");
        if (waitTimeStr != null) {
            m.diskWaitTime = Long.parseLong(waitTimeStr);
        }

        String serviceTimeStr = readDiskFileFormat(blkioPath + "blkio.io_service_time");
        if (serviceTimeStr != null) {
            m.diskServiceTime = Long.parseLong(serviceTimeStr);
        }
        String diskQueueStr = readDiskFileFormat(blkioPath + "blkio.io_queued");
        if (diskQueueStr != null) {
            m.diskQueued = Long.parseLong(diskQueueStr);
        }
        Long blkioTime = 0L;
        List<String> readLines = readFileLines(blkioPath + "blkio.time");
        if (readLines != null || readLines.size() >= 1) {
            for(String line: readLines) {
                try {
                    String wordInLine[] = line.split("\\s+");
                    blkioTime += Long.parseLong(wordInLine[1]);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        }
        m.diskIOTime = blkioTime;
        return calRate;
    }

    private String readDiskFileFormat(String url) {
        String res = null;
        List<String> readLines = readFileLines(url);
        if (readLines != null || readLines.size() >= 2) {
            String[] wordsInLine = readLines.get(readLines.size() - 1).split("\\s+");
            if(wordsInLine.length == 2) {
                try {
                    res = wordsInLine[1];
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        }
        return res;
    }

    // calculate the network I/O rate(kb/s)
    private void calculateCurrentNetMetric(DockerMetrics m) {
        if(!getNetServicedBytes(m)) {
            m.netTransBytes = 0L;
            m.netRecBytes = 0L;
            return;
        }
        Double deltaTime = (m.timestamp - previousMetrics.timestamp) / 1000.0;

        Long deltaReceive = m.netRecBytes - previousMetrics.netRecBytes;
        m.netRecRate = deltaReceive / deltaTime / 1000d;
        Long deltaTransmit = m.netTransBytes - previousMetrics.netTransBytes;
        m.netTransRate = deltaTransmit / deltaTime / 1000d;
        //System.out.print("deltaTime: " + deltaTime + " deltaRec: " + deltaReceive + " deltaTrans: " + deltaTransmit + "\n");
    }

    // parse the network usage from 'proc' files
    // and update the taskMetrics in the monitor.
    private boolean getNetServicedBytes(DockerMetrics m) {
        if (!isRunning) {
            return false;
        }
        boolean hasFirst = true;
        if (previousMetrics == null) {
            hasFirst = false;
        }
        String[] results = runShellCommand("cat " + netFilePath).split("\n");
        String resultLine = null;
        for (String r: results) {
            if (r.matches(".*"+ifaceName+".*")) {
                resultLine = r;
                break;
            }
        }


        // we get values that are in the file. but we need to subtract the first value we got.
        // we will fix it when this method return.
        if (resultLine != null && resultLine.length() > 0) {
            resultLine = resultLine.trim();
            String receiveStr = resultLine.split("\\s+")[1];
            if (netRecOffset == 0L) {
                netRecOffset = Long.parseLong(receiveStr);
            }
            m.netRecBytes = Long.parseLong(receiveStr) - netRecOffset;

            String transmitStr = resultLine.split("\\s+")[8];
            if (netTransOffset == 0L) {
                netTransOffset = Long.parseLong(transmitStr);
            }
            m.netTransBytes = Long.parseLong(transmitStr) - netTransOffset;
            //System.out.print("netRec: " + m.netRecBytes + " netTrans: " + m.netTransBytes + "\n");
        }
        return hasFirst;
    }

    private List<String> readFileLines(String path){
        ArrayList<String> results= new ArrayList<String>();
        File file = new File(path);
        BufferedReader reader = null;
        boolean isError=false;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                results.add(tempString);
            }
            reader.close();
        } catch (IOException e) {
            //if we come to here, then means parse file causes errors;
            //if reports this errors mission errors, it means this containers
            //has terminated, but nodemanager did not delete it yet. we stop monitoring
            //here
            if(e.toString().contains("FileNotFoundException")){
                isError = true;
            }
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }

        if(!isError){
            return results;
        }else{
            stop();
            manager.removeDockerMonitor(containerId);
            return null;
        }
    }

    private void sendInfoToDatabase() {
        if (currentMetrics == null) {
            return;
        }
        DockerMetrics last = currentMetrics;
    }

    // TEST
    public void printStatus() {
        if (currentMetrics == null) {
            return;
        }
        System.out.printf("docker pid: %s\n" +
                "cpu rate: %s\n" +
                "memory usage in kb: %d\n" +
                "total disk io bytes: %d disk io rate: %f\n" +
                "total receive: %d total transmit: %d receive rate: %s transmit rate: %s\n",
                dockerPid,
                currentMetrics.cpuRate,
                currentMetrics.memoryUsage,
                currentMetrics.diskServiceBytes,
                currentMetrics.diskRate,
                currentMetrics.netRecBytes, currentMetrics.netTransBytes,
                currentMetrics.netRecRate, currentMetrics.netTransRate);
    }
}
