package feedback;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Eddie on 2017/12/11.
 */
public class FeedbackManager {

    private static FeedbackManager ourInstance = new FeedbackManager();

    public static FeedbackManager getInstance() {
        return ourInstance;
    }

    private ConcurrentMap<String, AbstractFeedback> feedbackThreadPool;

    private FeedbackManager() {
        feedbackThreadPool = new ConcurrentHashMap<>();
    }

    public void registerFeedback(AbstractFeedback feedback) {

        if(feedbackThreadPool.putIfAbsent(feedback.name, feedback) == null) {
            Thread feedbackThread = new Thread(feedback);
            feedbackThread.start();
        }
    }

    public void unregisterFeedback(String name) {
        AbstractFeedback feedbackToRemove = feedbackThreadPool.remove(name);
        if (feedbackToRemove != null) {
            feedbackToRemove.stop();
        }
    }
}
