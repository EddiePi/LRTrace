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
        wm.instantiateWindow(3, 2);
        Long timeBoundary = 10L;
        Integer idBoundary = 4;
        Random CPURandom = new Random();
        Random memoryRandom = new Random();
        for (Long time = 0L; time < timeBoundary; time++) {
            for (Integer id = 0; id < idBoundary; id++) {
                AnalysisContainer container = wm.getContainerToAssign(time, id.toString());
                container.CPU = CPURandom.nextGaussian();
                container.memory = memoryRandom.nextDouble();
            }
        }
        timeBoundary = 25L;
        for (Long time = 15L; time < timeBoundary; time++) {
            for (Integer id = 0; id < idBoundary; id++) {
                AnalysisContainer container = wm.getContainerToAssign(time, id.toString());
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
    public void storeAndLoadSlidingWindow() throws Exception {
        wm.storeSlidingWindow();
        wm.slidingWindow.clear();
        System.out.print(wm.slidingWindow.size() + "\n");
        wm.loadSlidingWindow();
        System.out.print(wm.slidingWindow.size() + "\n");
    }

}