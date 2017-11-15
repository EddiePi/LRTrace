package log;

import logAPI.LogAPICollector;
import logAPI.MessageMark;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by Eddie on 2017/7/3.
 */
public class AppObjRecorder {
    List<MessageMark> allMarks;

    private static final AppObjRecorder instance = new AppObjRecorder();

    private AppObjRecorder(){
        LogAPICollector collector = LogAPICollector.getInstance();
        allMarks = collector.containerRuleMarkList;
    }

    public static AppObjRecorder getInstance() {
        return instance;
    }

    private ConcurrentMap<String, Map<String ,ObjInfoWithTimestamp>> timestampInfoMap = new ConcurrentHashMap<>();

    public void maybeUpdateInfo(String containerId, String message) {
        for(MessageMark mark: allMarks) {
            Pattern pattern = Pattern.compile(mark.regex);
            Matcher matcher = pattern.matcher(message);
            if(matcher.matches()) {
                System.out.printf("app log message is matched: %s.\n", message);
                if(matcher.groupCount() >= 1) {
                    for(MessageMark.Group group: mark.groups) {
                        String value = matcher.group(group.name).trim().replaceAll("\\s+", "_");
                        String[] words = message.split("\\s+");
                        Long timestamp = Timestamp.valueOf(words[0] + " " + words[1].replace(',', '.')).getTime();
                        System.out.printf("going to update app value. name: %s, value: %s, isFinish: %b\n",
                                group.name, value, group.isFinish);
                        updateInfo(containerId, group.name, value, timestamp, group.isFinish);
                    }
                }
            }
        }
    }

    public void updateInfo(String containerId, String name, String value, Long timestamp, boolean isFinish) {
        Map<String, ObjInfoWithTimestamp> nameToInfoMap =
                timestampInfoMap.getOrDefault(containerId, null);
        if(isFinish) {
            nameToInfoMap.remove(name);
        } else {
            if (nameToInfoMap== null) {
                nameToInfoMap = new HashMap<>();
                timestampInfoMap.put(containerId, nameToInfoMap);
            }
            ObjInfoWithTimestamp existingInfo = nameToInfoMap.get(name);
            if(existingInfo != null) {
                if(existingInfo.value.equals(value)) {
                } else {
                    System.out.printf("update existing info: %s\n", name);
                    existingInfo.value = value;
                    existingInfo.timestamp = timestamp;
                }
                return;
            }
            System.out.printf("add new info: %s\n", name);
            ObjInfoWithTimestamp objInfoWithTimestamp = new ObjInfoWithTimestamp(name, value, timestamp);
            nameToInfoMap.put(name, objInfoWithTimestamp);
        }
    }

    public List<String> getInfo(String containerId, Long timestamp) {
        List<String> res;
        Map<String, ObjInfoWithTimestamp> objInfoWithTimestampMap =
                timestampInfoMap.getOrDefault(containerId, null);
        if (objInfoWithTimestampMap == null) {
            return null;
        }
        res = new ArrayList<>();
        for(Map.Entry<String, ObjInfoWithTimestamp> entry: objInfoWithTimestampMap.entrySet()) {
            ObjInfoWithTimestamp value = entry.getValue();
            res.add(value.name + ":" + value.value);

        }
        return res;
    }


    private class ObjInfoWithTimestamp {
        Long timestamp;
        String name;
        String value;

        public ObjInfoWithTimestamp(String name, String value, Long timestamp) {
            this.timestamp = timestamp;
            this.name = name;
            this.value = value;
        }
    }
}
