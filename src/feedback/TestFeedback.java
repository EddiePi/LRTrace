package feedback;

import detection.AnalysisContainer;

import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/12/11.
 */
public class TestFeedback extends AbstractFeedback {

    public TestFeedback(String name, Integer interval) {
        super(name, interval);
    }

    @Override
    public void action(List<Map<String, AnalysisContainer>> data) {
        for (Map<String, AnalysisContainer> windowMap: data) {
            for(Map.Entry<String, AnalysisContainer> entry: windowMap.entrySet()) {
                System.out.printf("container id: %s, timestamp: %d\n", entry.getValue().getContainerId(), entry.getValue().getTimestamp());
            }
        }
    }
}
