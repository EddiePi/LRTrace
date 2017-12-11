package detection;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

/**
 * Created by Eddie on 2017/11/13.
 */
public class TrainerTest {
    Trainer trainer;
    WindowManager wm;

    @Before
    public void setUp() throws Exception {
        trainer = new Trainer();
        wm = WindowManager.getInstance();
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
        trainer.fetchAllWindowedData();
    }

    @Test
    public void preprocessData() throws Exception {

    }

    @Test
    public void flattenMetric() throws Exception {
        trainer.flattenMetric();
        int windowIndex = 0;
        for (Map<String, Double> map: trainer.windowedMetric) {
            for (Map.Entry<String, Double> entry: map.entrySet()) {
                System.out.printf("window: %d. Container: %s, avg: %f\n", windowIndex, entry.getKey(), entry.getValue());
            }
            windowIndex++;
        }
    }

}