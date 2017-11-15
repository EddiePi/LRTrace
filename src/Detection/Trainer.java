package Detection;

import Detection.Extractor.MemoryRateExtractor;
import Detection.Extractor.WindowedDataExtractor;

import java.util.*;

/**
 * Created by Eddie on 2017/11/9.
 */
public class Trainer {
    protected WindowManager wm = WindowManager.getInstance();

    protected List<List<Map<String, AnalysisContainer>>> allWindowedData;

    protected List<Map<String, Double>> windowedMetric;

    // List<Map(container to log)<containerId, Map<log key, number>
    protected List<Map<String, Map<String, Integer>>> windowedLogKey;

    protected Set<String> periodKeySet;

    // this is set by other class.
    public WindowedDataExtractor extractor = new MemoryRateExtractor();

    public Trainer() {
        allWindowedData = new ArrayList<>();
        periodKeySet = new HashSet<>();
        windowedLogKey = new ArrayList<>();
    }

    public void preprocessData() {
        fetchAllWindowedData();
        preProcessLog();

    }

    // public for test
    public void fetchAllWindowedData() {
        while (wm.hasMoreData()) {
            allWindowedData.add(wm.getWindowedDataForAnalysis());
        }
    }

    // public for test
    public void preProcessLog() {
        for (List<Map<String, AnalysisContainer>> oneWindow: allWindowedData) {
            // window level
            Map<String, Map<String, Integer>> containerToKeyToNumber = new HashMap<>();
            for(Map<String, AnalysisContainer> containerMap: oneWindow) {
                // container level

                for(AnalysisContainer container: containerMap.values()) {
                    Map<String, Integer> keyToNumber = new HashMap<>();
                    for (KeyedMessage message: container.periodMessages) {
                        periodKeySet.add(message.key);
                        String key = message.key;
                        Integer number = keyToNumber.get(key);
                        if (number != null) {
                            number++;
                        } else {
                            number = 1;
                        }
                        keyToNumber.put(key, number);
                    }
                    containerToKeyToNumber.put(container.getContainerId(), keyToNumber);
                }

            }
            windowedLogKey.add(containerToKeyToNumber);
        }
    }

    // public for test
    public void flattenMetric() {
        windowedMetric = extractor.extract(allWindowedData);
        windowedMetric.isEmpty();
    }

    public void train() {

    }
}
