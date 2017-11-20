package KafkaUniform.window;

import Detection.AnalysisContainer;
import Detection.WindowManager;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/11/15.
 */
public class WindowChannelTest {

    WindowManager windowManager;
    WindowChannel windowChannel;

    @Before
    public void setUp() throws Exception {
        windowManager = WindowManager.getInstance();
        windowChannel = new WindowChannel();
    }

    @Test
    public void updateMetric() throws Exception {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("container", "01_000001");
        testMap.put("app", "1");
        Long timestamp = 1l;
        for (int i = 0; i < 10; i++) {
            windowChannel.updateMetric("cpu", timestamp++, 0.3d, testMap);
        }
        windowManager.slidingWindow.isEmpty();
    }

    @Test
    public void updateLog() throws Exception {
        Map<String, String> testMap1 = new HashMap<>();
        testMap1.put("container", "01_000001");
        testMap1.put("app", "app1");
        testMap1.put("task", "t1");
        testMap1.put("state", "s1");

        Map<String, String> testMap2 = new HashMap<>();
        testMap2.put("container", "01_000002");
        testMap2.put("app", "app1");
        testMap2.put("task", "t2");
        testMap2.put("state", "s1");
        Long timestamp = 1l;
        for (int i = 0; i < 10; i++) {
            windowChannel.updateLog("period:task", timestamp, 1d, testMap1);
            windowChannel.updateLog("period:task", timestamp++, 1d, testMap2);
        }
        List<List<Map<String, AnalysisContainer>>> allWindowedData = new ArrayList<>();
        while(!windowManager.slidingWindow.isEmpty()) {
            allWindowedData.add(windowManager.getWindowedDataForAnalysis());
        }
        allWindowedData.isEmpty();
    }

}