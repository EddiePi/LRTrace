package feedback;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/12/13.
 */
public class FeedbackLoaderTest {
    FeedbackLoader feedbackLoader;
    @Before
    public void setUp() throws Exception {
        feedbackLoader = new FeedbackLoader();
    }

    @Test
    public void load() throws Exception {
        feedbackLoader.load();
        Thread.sleep(6000);
    }

}