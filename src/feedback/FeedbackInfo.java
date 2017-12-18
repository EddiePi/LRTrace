package feedback;

import java.net.URL;

/**
 * Created by Eddie on 2017/12/13.
 */
public class FeedbackInfo {
    public URL path;
    public String className;
    public Integer interval;

    public FeedbackInfo(URL path, String className, Integer interval) {
        this.path = path;
        this.className = className;
        this.interval = interval;
    }
}
