package log;

import logAPI.LogAPICollector;
import logAPI.MessageMark;
import logAPI.XMLParser;
import org.junit.Before;
import org.junit.Test;
import KafkaUniform.tsdb.PackedMessage;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Eddie on 2017/7/25.
 */
public class ContainerLogReaderTest {

    List<String> testLog;
    LogAPICollector collector;
    @Before
    public void setUp() throws Exception {
        collector = LogAPICollector.getInstance();
        collector.containerRuleMarkList.addAll(XMLParser.parse("/Users/Eddie/gitRepo/tracing-server-2.0/conf/Testing-api.xml"));
        testLog = new ArrayList<>();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date nowDate = new Date();
        String now = format.format(nowDate);
        testLog.add("2017-07-25 23:57:16.357 INFO Executor: Running task 126.0 in stage 0.0 (TID 123)");
        testLog.add("2017-07-25 23:57:16.352 INFO Executor: Finished task 69.0 in stage 0.0 (TID 68). 1665 bytes result sent to driver");
    }

    @Test
    public void maybePackMessage() throws Exception {
        List<PackedMessage> packedMessagesList = new ArrayList<>();
        for (String logMessage : testLog) {
            for (MessageMark messageMark : collector.containerRuleMarkList) {
                Pattern pattern = Pattern.compile(messageMark.regex);
                Matcher matcher = pattern.matcher(logMessage);
                if (matcher.matches()) {
                    for (MessageMark.Group group : messageMark.groups) {
                        try {
                            String name = group.name;
                            String valueStr = group.value;
                            Long timestamp = LogReaderManager.parseTimestamp(logMessage) + messageMark.dateOffset;
                            Double value;
                            if (valueStr.matches("^[-+]?[\\d]*(\\.\\d*)?$")) {
                                value = Double.valueOf(valueStr);
                            } else {
                                value = Double.valueOf(matcher.group(valueStr));
                            }
                            Map<String, String> tagMap = new HashMap<>();
                            for (String tagName : group.tags) {
                                String tagValue = matcher.group(tagName);
                                tagMap.put(tagName, tagValue);
                            }
                            PackedMessage packedMessage =
                                    new PackedMessage("container_1", timestamp, name, tagMap, value);
                            packedMessagesList.add(packedMessage);
                        } catch (IllegalStateException e) {
                            e.printStackTrace();
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        for(PackedMessage packedMessage: packedMessagesList) {
            System.out.print(packedMessage + "\n");
        }
    }

}