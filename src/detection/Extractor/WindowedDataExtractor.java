package detection.Extractor;

import detection.AnalysisContainer;

import java.util.List;
import java.util.Map;

/**
 * this class extract the designated data in one window.
 */
public interface WindowedDataExtractor {
    public List<Map<String, Double>> extract(List<List<Map<String, AnalysisContainer>>> allWindowedData);
}
