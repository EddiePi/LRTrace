package KafkaUniform.tsdb;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by Eddie on 2017/6/24.
 */
public class TsdbMetricBuilderTest {

    TsdbMetricBuilder builder = TsdbMetricBuilder.getInstance();
    @Before
    public void setUp() throws Exception {
        builder.addMetric("test")
                .setDataPoint(System.currentTimeMillis(), 1)
                .addTag("host", "node1")
                .addTag("container", "container1");
        Thread.sleep(1000);
        builder.addMetric("test")
                .setDataPoint(System.currentTimeMillis(), 2)
                .addTag("host", "node1")
                .addTag("container", "container2");
    }

    @Test
    public void addMetric() throws Exception {

    }

    @Test
    public void build() throws Exception {
        String jsonRes;
        jsonRes = builder.build(true);
        System.out.printf("the build result is: %s.\n", jsonRes);
    }

}