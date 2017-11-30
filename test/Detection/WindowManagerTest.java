package Detection;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by Eddie on 2017/11/9.
 */
public class WindowManagerTest {

    WindowManager wm;
    @Before
    public void setUp() throws Exception {
        wm = WindowManager.getInstance();
        Long timeBoundary = 10L;
        Integer idBoundary = 4;
        Random CPURandom = new Random();
        Random memoryRandom = new Random();
        for (Long time = 0L; time < timeBoundary; time++) {
            for (Integer id = 0; id < idBoundary; id++) {
                AnalysisContainer container = wm.getContainerToAssign(time, "container_" + id.toString());
                container.CPU = CPURandom.nextGaussian();
                container.memory = memoryRandom.nextDouble();
            }
        }
        timeBoundary = 25L;
        for (Long time = 15L; time < timeBoundary; time++) {
            for (Integer id = 0; id < idBoundary; id++) {
                AnalysisContainer container = wm.getContainerToAssign(time, "container_" + id.toString());
                container.CPU = CPURandom.nextGaussian();
                container.memory = memoryRandom.nextDouble();
            }
        }
    }


    @Test
    public void getDataForAnalysis() throws Exception {
        List<List<Map<String, AnalysisContainer>>> results = new ArrayList<>();
        for(int i = 0; i < 15; i++) {
            results.add(wm.getWindowedDataForAnalysis());
        }
        results.isEmpty();
    }

    @Test
    public void getContainerToAssign() throws Exception {

    }

    @Test
    public void loadSlidingWindow() throws Exception {
        wm.loadSlidingWindow();
        for (Map.Entry<Long, Map<String, AnalysisContainer>> entry: wm.slidingWindow.entrySet()) {
            System.out.printf("*time window: %d\n", entry.getKey());
            for (Map.Entry<String, AnalysisContainer> containerEntry: entry.getValue().entrySet()) {
                System.out.printf("**containerId: %s\n", containerEntry.getKey());
                if (containerEntry.getValue().periodMessages.size() > 0) {
                    System.out.printf("***period: \n");
                    for (String messageKey : containerEntry.getValue().periodMessages.keySet()) {
                        System.out.printf("****period key: %s\n", messageKey);
                    }
                }
                if (containerEntry.getValue().instantMessages.size() > 0) {
                    System.out.printf("***instant: \n");
                    for (String messageKey : containerEntry.getValue().instantMessages.keySet()) {
                        System.out.printf("****instant key: %s\n", messageKey);
                    }
                }
            }
        }
    }

    @Test
    public void storeAndLoadSlidingWindow() throws Exception {
        wm.storeSlidingWindow();
        wm.slidingWindow.clear();
        System.out.print(wm.slidingWindow.size() + "\n");
        wm.loadSlidingWindow();
        System.out.print(wm.slidingWindow.size() + "\n");
    }

    @Test
    public void storeSlidingWindowAsJson() throws Exception {
        wm.storeSlidingWindowAsJson();
    }

}