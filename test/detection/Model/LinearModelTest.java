package detection.Model;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Eddie on 2017/11/8.
 */
public class LinearModelTest {

    LinearModel linearModel;
    @Before
    public void setUp() throws Exception {
        List<Double> memoryPerContainer1 = Arrays.asList(0.4, 0.25, 0.25, 0.25, 1.38, 1.37, 1.4, 2.9, 1.8, 0.78, 0.25, 0.02, 0.15, 0.15,
                0.03, 0.0, 0.0, 1.75, 0.0, 0.0, 0.0);
        List<Double> taskPerContainer1 = Arrays.asList(1.0, 1.0, 1.0, 1.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 2.0,1.0, 1.0, 0.0,
                0.0, 1.0, 1.0, 1.0, 1.0);
        linearModel = new LinearModel(taskPerContainer1, memoryPerContainer1);
    }

    @Test
    public void startAnalysis() throws Exception {
    }

    @Test
    public void getConfidenceInterval() throws Exception {
        Double interval[] = linearModel.getConfidenceInterval(3d);
        System.out.printf("interval: %f, %f\n", interval[0], interval[1]);
    }

}