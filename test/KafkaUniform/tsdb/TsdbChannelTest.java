package KafkaUniform.tsdb;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/11/16.
 */
public class TsdbChannelTest {

    TsdbChannel tsdbChannel;

    @Before
    public void setUp() throws Exception {
        tsdbChannel = new TsdbChannel();
    }

    @Test
    public void updateLog() throws Exception {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("container", "container_1502222838299_0001_01_000002");
        tsdbChannel.updateLog("task", System.currentTimeMillis(), 1d, testMap);
    }

    @Test
    public void updateMetric() throws Exception {
    }

}