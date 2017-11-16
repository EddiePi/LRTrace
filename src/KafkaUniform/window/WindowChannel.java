package KafkaUniform.window;

import Detection.AnalysisContainer;
import Detection.KeyedMessage;
import Detection.WindowManager;
import KafkaUniform.KafkaChannel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/11/14.
 */
public class WindowChannel implements KafkaChannel {

    WindowManager wm;

    public WindowChannel() {
        wm = WindowManager.getInstance();
    }

    @Override
    public void updateMetric(String metricType, Long timestamp, Double value, Map<String, String> tags) {
        AnalysisContainer containerToUpdate = wm.getContainerToAssign(timestamp, tags.get("container"));
        containerToUpdate.setTimestamp(timestamp);
        assignId(containerToUpdate, tags);
        switch (metricType) {
            case "cpu": containerToUpdate.CPU = value; break;
            case "memory": containerToUpdate.memory = value; break;
            case "disk.service.byte": containerToUpdate.diskServiceByte = value; break;
            case "disk.service.time": containerToUpdate.diskServiceTime = value; break;
            case "disk.wait.time": containerToUpdate.diskWaitTime = value; break;
            case "diskIOTime": containerToUpdate.diskIOTime = value; break;
            case "network": containerToUpdate.netRate = value; break;
        }
    }

    @Override
    public void updateLog(String key, Long timestamp, Double value, Map<String, String> tags) {
        AnalysisContainer containerToUpdate = wm.getContainerToAssign(timestamp, tags.get("container"));
        containerToUpdate.setTimestamp(timestamp);
        assignId(containerToUpdate, tags);
        assignKeyedMessage(containerToUpdate, key, tags);
    }

    private void assignId(AnalysisContainer container, Map<String, String> tags) {
        for(Map.Entry<String, String> entry: tags.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equals("container")) {
                container.setContainerId(value);
            } else if (key.equals("app")) {
                container.setAppId(value);
            }
        }
    }

    private void assignKeyedMessage(AnalysisContainer container, String messageKey, Map<String, String> tags) {
        KeyedMessage newMessage = new KeyedMessage();
        Map<String, List<KeyedMessage>> messageMap;
        String[] typeName = messageKey.split(":");
        if (typeName.length < 2) {
            return;
        }
        if (typeName[0].equals("period")) {
            messageMap = container.periodMessages;
        } else {
            messageMap = container.instantMessages;
        }
        newMessage.key = typeName[1];

        List<KeyedMessage> messageListToUpdate = messageMap.getOrDefault(typeName[1], new ArrayList<>());
        for(Map.Entry<String, String> entry: tags.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equals("container") || key.equals("app")) {
                continue;
            } else {
                newMessage.identifiers.put(key, value);
            }
        }
        messageListToUpdate.add(newMessage);
        messageMap.put(typeName[1], messageListToUpdate);
    }
}
