package feedback;

import detection.AnalysisContainer;
import detection.WindowManager;

import java.util.List;
import java.util.Map;

/**
 * Created by Eddie on 2017/12/11.
 */
public abstract class AbstractFeedback implements Runnable {
    WindowManager windowManager = WindowManager.getInstance();
    FeedbackManager feedbackManager = FeedbackManager.getInstance();

    public String name;
    private Integer feedbackInterval;
    private Boolean isRunning;
    // the structure of the list: List -> each window -> containers in the window.
    // all data are raw and the list should be cleared by the UDF.
    protected List<Map<String, AnalysisContainer>> dataList;

    public AbstractFeedback(String name, Integer interval) {
        this.feedbackInterval = interval;
        isRunning = true;

        feedbackManager.registerFeedback(this);
    }

    @Override
    public void run() {
        while(isRunning) {
            dataList = windowManager.getWindowedDataForAnalysis(System.currentTimeMillis() - 2000);
            action(dataList);
            try {
                Thread.sleep(feedbackInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        isRunning = false;
        feedbackManager.unregisterFeedback(name);
    }

    public abstract void action(List<Map<String, AnalysisContainer>> data);
}
