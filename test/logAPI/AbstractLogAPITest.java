package logAPI;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/7/6.
 */
public class AbstractLogAPITest {
    AbstractLogAPI api = new SparkLogAPI();
    @Before
    public void setUp() throws Exception {
        api.apiFile = new File("conf/spark-api.xml.template");
    }

    @Test
    public void parseFile() throws Exception {
        api.parseFile();
    }

}