package Detection.Extractor;

import Detection.AnalysisContainer;
import Detection.KeyedMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/11/10.
 */
public class MemoryRateExtractor implements WindowedDataExtractor {

    @Override
    public List<Map<String, Double>> extract(List<List<Map<String, AnalysisContainer>>> allWindowedData) {
        List<Map<String, Double>> windowedMemoryRateList = new ArrayList<>();

        for (List<Map<String, AnalysisContainer>> oneWindow: allWindowedData) {
            // window level
            int dataCount = oneWindow.size();
            if (dataCount == 0) {
                continue;
            }
            Map<String, Double> containerToMemoryRate = new HashMap<>();

            for(Map<String, AnalysisContainer> containerMap: oneWindow) {
                // container level
                for (AnalysisContainer container : containerMap.values()) {
                    Double avgSumCalculating = containerToMemoryRate.get(container.getContainerId());
                    if (avgSumCalculating == null) {
                        avgSumCalculating = 0d;
                    }
                    avgSumCalculating += container.memory;
                    containerToMemoryRate.put(container.getContainerId(), avgSumCalculating);
                }
            }
            for (Map.Entry<String, Double> entry: containerToMemoryRate.entrySet()) {
                Double avg = entry.getValue() / dataCount;
                containerToMemoryRate.put(entry.getKey(), avg);
            }
            windowedMemoryRateList.add(containerToMemoryRate);
        }
        return windowedMemoryRateList;
    }
}
