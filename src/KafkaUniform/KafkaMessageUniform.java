package KafkaUniform;

import KafkaUniform.tsdb.PackedMessage;
import KafkaUniform.tsdb.StateCollection;
import KafkaUniform.tsdb.TsdbChannel;
import KafkaUniform.window.WindowChannel;
import Server.TracerConf;
import log.LogReaderManager;
import logAPI.LogAPICollector;
import logAPI.MessageMark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Eddie on 2017/11/14.
 */
public class KafkaMessageUniform {
    Properties props;
    KafkaConsumer<String, String> consumer;
    List<String> kafkaTopics;
    TracerConf conf = TracerConf.getInstance();
    PullRunnable pullRunnable;
    Thread transferThread;


    LogAPICollector collector = LogAPICollector.getInstance();

    final Map<String, List<PackedMessage>> eventMessagesMap;


    // if a event lasts less than 1s, it might be cleared before gets sent.
    // we use this list to store this kind of message
    final List<PackedMessage> shortEventMessageList;

    List<KafkaChannel> channelList;

    public KafkaMessageUniform() {
        eventMessagesMap = new HashMap<>();
        shortEventMessageList = new LinkedList<>();
        props = new Properties();
        props.put("bootstrap.servers", conf.getStringOrDefault("tracer.kafka.bootstrap.servers", "localhost:9092"));
        props.put("group.id", "trace");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        kafkaTopics = Arrays.asList("trace", "log");
        consumer.subscribe(kafkaTopics);


        pullRunnable = new PullRunnable();
        transferThread = new Thread(pullRunnable);
    }

    private void initAllChannel() {
        channelList = new ArrayList<>();
        String[] confArr = conf.getStringOrDefault("tracer.channels", "tsdb, window").toLowerCase().split(",");
        for (String channel: confArr) {
            switch (channel.trim()) {
                case "tsdb": channelList.add(new TsdbChannel());break;
                case "window": channelList.add(new WindowChannel());break;
            }
        }


    }

    private class PullRunnable implements Runnable {
        boolean isRunning = true;

        @Override
        public void run() {
            while (isRunning) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                boolean hasMessage = false;
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    if (value.matches("container.* is finished\\.")) {
                        removeEventMessage(value.split(" ")[0]);
                        continue;
                    }
                    if (key.matches("testlog-log")) {
                        sendTestMessage(value);
                        continue;
                    }
                    if (key.matches("container.*-metric")) {
                        hasMessage = hasMessage | metricTransformer(value);
                    } else if (key.matches("container.*-log")) {
                        hasMessage = hasMessage | containerLogTransformer(value);
                    } else if (key.equals("nodemanager-log")) {
                        hasMessage = hasMessage | managerLogTransformer(value);
                    } else if (key.equals("resourcemanager-log")) {
                        hasMessage = hasMessage | maybeBuildRMMessage(value);
                    } else {
                        System.out.printf("unrecognized kafka key: %s\n", key);
                    }
                }
            }
            consumer.close(5, TimeUnit.SECONDS);
        }
    }

    public void start() {
        transferThread.start();
    }

    public void stop() {
        pullRunnable.isRunning = false;
        eventMessagesMap.clear();
    }

    private boolean metricTransformer(String metricStr) {
        String[] metrics = metricStr.split(",");
        if (metrics.length < 8) {
            return false;
        }
        //System.out.printf("metricStr: %s\n", metricStr);
        try {
            Long timestamp = Timestamp.valueOf(metrics[1]).getTime();
            Double cpuUsage = Double.valueOf(metrics[2]);
            Double memoryUsage = Double.valueOf(metrics[3]);
            Double diskServiceByte = Double.valueOf(metrics[4]);
            Double diskServiceTime = Double.valueOf(metrics[5]);
            Double diskWaitTime = Double.valueOf(metrics[6]);
            Double diskIOTime = Double.valueOf(metrics[7]);
            Double netRate = Double.valueOf(metrics[8]) + Double.valueOf(metrics[9]);
            Map<String, String> tagMap = buildAllTags(metrics);
            for (KafkaChannel channel: channelList) {
                channel.updateMetric("cpu", timestamp, cpuUsage, tagMap);
                channel.updateMetric("memory", timestamp, memoryUsage, tagMap);
                channel.updateMetric("disk.service.byte", timestamp, diskServiceByte, tagMap);
                channel.updateMetric("disk.service.time", timestamp, diskServiceTime, tagMap);
                channel.updateMetric("disk.wait.time", timestamp, diskWaitTime, tagMap);
                channel.updateMetric("diskIOTime", timestamp, diskIOTime, tagMap);
                channel.updateMetric("network", timestamp, netRate, tagMap);
            }

        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }

        return true;
    }

    private boolean containerLogTransformer(String kafkaMessage) {
        List<PackedMessage> packedMessageList;
        packedMessageList = maybePackContainerMessage(kafkaMessage);

        if (packedMessageList == null) {
            return false;
        }
        if (packedMessageList.size() == 0) {
            return false;
        }
        buildPackedMessage(packedMessageList);
        return true;
    }

    private List<PackedMessage> maybePackContainerMessage(String kafkaMessage) {
        int separatorIndex = kafkaMessage.indexOf(' ');
        if (separatorIndex <= 0) {
            return null;
        }
        String logMessage = kafkaMessage.substring(separatorIndex).trim();
        String componentId = kafkaMessage.substring(0, separatorIndex).trim();
        if (!componentId.matches("(container.*)")) {
            return null;
        }
        List<PackedMessage> packedMessagesList = new ArrayList<>();
        for (MessageMark messageMark : collector.containerRuleMarkList) {
            Pattern pattern = Pattern.compile(messageMark.regex);
            Matcher matcher = pattern.matcher(logMessage);
            if (matcher.matches()) {
                // System.out.printf("matched log: %s\n", logMessage);
                for (MessageMark.Group group : messageMark.groups) {
                    try {
                        String name = group.name;
                        String valueStr = group.value;
                        String type = group.type;
                        Long timestamp = LogReaderManager.parseTimestamp(logMessage) + messageMark.dateOffset;
                        Double value;
                        if (valueStr.matches("^[-+]?[\\d]*(\\.\\d*)?$")) {
                            value = Double.valueOf(valueStr);
                        } else {
                            String valueWithUnit = matcher.group(valueStr);
                            value = parseDoubleStrWithUnit(valueWithUnit);
                        }
                        Map<String, String> tagMap = new HashMap<>();
                        for (String tagName : group.tags) {
                            String tagValue = matcher.group(tagName).replaceAll("\\s|#", "_");
                            tagMap.put(tagName, tagValue);
                        }
                        PackedMessage packedMessage =
                                new PackedMessage(componentId, timestamp, name, tagMap, value, type);
                        if (type.equals("state")) {
                            packedMessagesList.add(packedMessage);
                        } else {
                            packedMessage.isFinish = group.isFinish;
                            updateEventMessage(packedMessage);
                        }
                        // TEST
                        // System.out.printf("packed message: %s\n", packedMessage);
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return packedMessagesList;
    }

    private boolean managerLogTransformer(String kafkaMessage) {
        List<PackedMessage> packedMessageList;
        packedMessageList = maybePackNMMessage(kafkaMessage);
        if (packedMessageList == null) {
            return false;
        }
        if (packedMessageList.size() == 0) {
            return false;
        }
        buildPackedMessage(packedMessageList);
        return true;
    }

    private List<PackedMessage> maybePackNMMessage(String kafkaMessage) {
        String logMessage = kafkaMessage;
        List<PackedMessage> packedMessagesList = new ArrayList<>();
        for (MessageMark messageMark : collector.managerRuleMarkList) {
            Pattern pattern = Pattern.compile(messageMark.regex);
            Matcher matcher = pattern.matcher(logMessage);
            if (matcher.matches()) {
                for (MessageMark.Group group : messageMark.groups) {
                    try {
                        String name = group.name;
                        String valueStr = group.value;
                        String type = group.type;
                        Long timestamp = LogReaderManager.parseTimestamp(logMessage) + messageMark.dateOffset;
                        Double value = null;
                        String containerId = "";
                        if (!name.equals("state")) {
                            if (valueStr.matches("^[-+]?[\\d]*(\\.\\d*)?$")) {
                                value = Double.valueOf(valueStr);
                            } else {
                                String valueWithUnit = matcher.group(valueStr);
                                value = parseDoubleStrWithUnit(valueWithUnit);
                            }
                        }
                        Map<String, String> tagMap = new HashMap<>();
                        for (String tagName : group.tags) {
                            String tagValue = matcher.group(tagName).replaceAll("\\s|#", "_");
                            if (tagName.equals("container")) {
                                containerId = tagValue;
                            } else if (!tagName.equals("state")) {
                                tagMap.put(tagName, tagValue);
                            }
                            // if we the metric name is 'state', we must have a tag also named 'state'.
                            if (name.equals("state") && tagName.equals("state")) {
                                Integer stateIntValue = StateCollection.containerStateMap.get(tagValue);
                                if (stateIntValue == null) {
                                    System.out.printf("unrecognized container state:%s\n", tagValue);
                                    continue;
                                }
                                value = (double) stateIntValue;
                            }
                        }
                        if (value != null) {
                            PackedMessage packedMessage =
                                    new PackedMessage(containerId, timestamp, name, tagMap, value == null ? 1d : value, type);
                            if (type.equals("state")) {
                                packedMessagesList.add(packedMessage);
                            } else {
                                packedMessage.isFinish = group.isFinish;
                                updateEventMessage(packedMessage);
                            }
                        }
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return packedMessagesList;
    }

    private void buildEventMessage() {
        Long timestamp = System.currentTimeMillis();
        synchronized (this.eventMessagesMap) {
            for (List<PackedMessage> mList : eventMessagesMap.values()) {
                for (PackedMessage m : mList) {
                    m.firstSend = false;
                    if (!m.containerId.equals("")) {
                        m.tagMap.put("container", parseShortContainerId(m.containerId));
                        m.tagMap.put("app", containerIdToShortAppId(m.containerId));
                    }
                    sendLogToAllChannel(m.name, timestamp, m.doubleValue,m.tagMap);
                }
            }
        }

        synchronized (this.shortEventMessageList) {
            for (PackedMessage m : shortEventMessageList) {
                if (!m.containerId.equals("")) {
                    m.tagMap.put("container", parseShortContainerId(m.containerId));
                    m.tagMap.put("app", containerIdToShortAppId(m.containerId));
                }
                sendLogToAllChannel(m.name, timestamp, m.doubleValue,m.tagMap);
            }
            shortEventMessageList.clear();
        }
    }

    private void updateEventMessage(PackedMessage message) {
        int index = hasEventMessage(message);
        synchronized (this.eventMessagesMap) {
            List<PackedMessage> packedMessageList;
            if (index < 0 && !message.isFinish) {
                packedMessageList = eventMessagesMap.getOrDefault(message.containerId, new ArrayList<>());
                packedMessageList.add(message);
                eventMessagesMap.put(message.containerId, packedMessageList);
            } else if (index >= 0 && message.isFinish) {
                PackedMessage oldMessage =
                        eventMessagesMap.get(message.containerId).remove(index);
                if (oldMessage.firstSend) {
                    synchronized (this.shortEventMessageList) {
                        shortEventMessageList.add(oldMessage);
                    }
                }
            }
        }
    }

    private void removeEventMessage(String key) {
        synchronized (this.eventMessagesMap) {
            eventMessagesMap.remove(key);
        }
    }

    /**
     * check if we already record the event message in <code>eventMessagesMap</code>
     *
     * @param message
     * @return if we find the message, return the index; otherwise return -1
     */
    private int hasEventMessage(PackedMessage message) {
        int index = -1;
        List<PackedMessage> packedMessagesInContainer;
        if ((packedMessagesInContainer = eventMessagesMap.get(message.containerId)) != null) {
            for (int i = 0; i < packedMessagesInContainer.size(); i++)
                if (packedMessagesInContainer.get(i).isCounterPart(message)) {
                    index = i;
                    break;
                }
        }

        return index;
    }

    private boolean maybeBuildRMMessage(String kafkaMessage) {
        String logMessage = kafkaMessage;
        boolean hasMessage = false;
        for (MessageMark messageMark : collector.managerRuleMarkList) {
            Pattern pattern = Pattern.compile(messageMark.regex);
            Matcher matcher = pattern.matcher(logMessage);
            if (matcher.matches()) {
                //System.out.printf("matched manager log: %s\n", logMessage);
                for (MessageMark.Group group : messageMark.groups) {
                    try {
                        String name = group.name;
                        String valueStr = group.value;
                        Long timestamp = LogReaderManager.parseTimestamp(logMessage) + messageMark.dateOffset;
                        Double value = null;
                        String appId = "";
                        String appAttemptId = "";
                        if (!name.equals("app.state") &&
                                !name.equals("app.attempt.state") &&
                                !name.equals("rm.container")) {
                            if (valueStr.matches("^[-+]?[\\d]*(\\.\\d*)?$")) {
                                value = Double.valueOf(valueStr);
                            } else {
                                String valueWithUnit = matcher.group(valueStr);
                                value = parseDoubleStrWithUnit(valueWithUnit);
                            }
                        }
                        Map<String, String> tagMap = new HashMap<>();
                        for (String tagName : group.tags) {
                            String tagValue = matcher.group(tagName).replaceAll("\\s|#", "_");
                            // if the metric's name is 'state', we must have a tag also named 'state'.
                            if (tagName.equals("state")) {
                                if (name.equals("app.state")) {
                                    Integer stateIntValue = StateCollection.RMAppState.get(tagValue);
                                    if (stateIntValue == null) {
                                        System.out.printf("unrecognized app state:%s\n", tagValue);
                                        continue;
                                    }
                                    value = (double) stateIntValue;
                                } else if (name.equals("app.attempt.state")) {
                                    Integer stateIntValue = StateCollection.RMAppAttemptStateMap.get(tagValue);
                                    if (stateIntValue == null) {
                                        System.out.printf("unrecognized appattemp state:%s\n", tagValue);
                                        continue;
                                    }
                                    value = (double) stateIntValue;
                                } else if (name.equals("rm.container.state")) {
                                    Integer stateIntValue = StateCollection.RMContainerState.get(tagValue);
                                    if (stateIntValue == null) {
                                        System.out.printf("unrecognized container state:%s\n", tagValue);
                                        continue;
                                    }
                                    value = (double) stateIntValue;
                                }
                            } else if (tagName.equals("app")) {
                                tagMap.put("app", tagValue);
                            } else if (tagName.equals("appAttempt")) {
                                appId = appAttemptIdToShortAppId(tagValue);
                                appAttemptId = parseShortAppAttemptId(tagValue);
                                tagMap.put("app", appId);
                                tagMap.put("app.attempt", appAttemptId);
                            } else if (tagName.equals("container")) {
                                appId = containerIdToShortAppId(tagValue);
                                String shortContainerId = parseShortContainerId(tagValue);
                                tagMap.put("app", appId);
                                tagMap.put("container", shortContainerId);
                            }
                        }
                        if (value != null) {
                            sendLogToAllChannel(name, timestamp, value, tagMap);
                            hasMessage = true;
                        }
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return hasMessage;
    }

    private void buildPackedMessage(List<PackedMessage> packedMessageList) {
        for (PackedMessage packedMessage : packedMessageList) {
            String appId = containerIdToShortAppId(packedMessage.containerId);
            if (!packedMessage.containerId.equals("")) {
                packedMessage.tagMap.put("container", parseShortContainerId(packedMessage.containerId));
                packedMessage.tagMap.put("app", appId);
            }
            sendLogToAllChannel(packedMessage.name, packedMessage.timestamp, packedMessage.doubleValue, packedMessage.tagMap);
        }
    }

    private Map<String, String> buildAllTags(String[] metrics) {
        String containerId = metrics[0];
        String appId = containerIdToShortAppId(containerId);
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("app", appId);
        tagMap.put("container", parseShortContainerId(containerId));
        if (metrics.length > 9) {
            for (int i = 9; i < metrics.length; i++) {
                String[] tagNValue = metrics[i].split(":");
                if (tagNValue.length < 2) {
                    continue;
                }
                tagMap.put(tagNValue[0], tagNValue[1]);
            }
        }
        return tagMap;
    }

    private void sendTestMessage(String line) {
        try {
            Long time = Long.parseLong(line);
            Date logDate = new Date(time);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Long curTime = System.currentTimeMillis();
            Long deltaTime = curTime - time;
            System.out.printf(deltaTime + "\n");
            Map<String, String> testMap = new HashMap<>();
            testMap.put("container", "01_000001");
            sendLogToAllChannel("test", System.currentTimeMillis(), 1D, testMap);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    private Double parseDoubleStrWithUnit(String doubleStr) {
        String tmp = doubleStr.trim();
        int firstCharIndex = 0;
        Double res;
        for (int i = tmp.length() - 1; i >= 0; i--) {
            if (tmp.charAt(i) >= '0' && tmp.charAt(i) <= '9') {
                firstCharIndex = i + 1;
                break;
            }
        }
        if (firstCharIndex == tmp.length()) {
            res = Double.valueOf(tmp);
        } else {
            String unit = tmp.substring(firstCharIndex).trim().toLowerCase();
            res = Double.valueOf(tmp.substring(0, firstCharIndex));
            switch (unit) {
                case "kb":
                    res *= 1024;
                    break;
                case "mb":
                    res *= 1024 * 1024;
                    break;
                case "gb":
                    res *= 1024 * 1024 * 1024;
                    break;
            }
        }
        return res;
    }

    private void sendLogToAllChannel(String key, Long timestamp, Double value, Map<String, String> tags) {
        for(KafkaChannel channel: channelList) {
            channel.updateLog(key, timestamp, value, tags);
        }
    }

    private void sendMetricToAllChannel(String metricType, Long timestamp, Double value, Map<String, String> tags) {
        for(KafkaChannel channel: channelList) {
            channel.updateMetric(metricType, timestamp, value, tags);
        }
    }

    private String parseShortContainerId(String containerId) {
        String[] parts = containerId.split("_");
        String shortId = parts[parts.length - 2] + "_" + parts[parts.length - 1];
        return shortId;
    }

    private String containerIdToShortAppId(String containerId) {
        String[] parts = containerId.split("_");
        String appId = parts[parts.length - 4] + "_" + parts[parts.length - 3];
        return appId;
    }

    private String parseShortAppAttemptId(String appAttemptId) {
        String[] parts = appAttemptId.split("_");
        String shortId = parts[parts.length - 1];
        return shortId;
    }

    private String appAttemptIdToShortAppId(String appAttemptId) {
        String[] parts = appAttemptId.split("_");
        String appId = parts[parts.length - 3] + "_" + parts[parts.length - 2];
        return appId;
    }
}

