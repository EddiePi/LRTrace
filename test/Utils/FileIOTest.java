package Utils;

import org.junit.Test;

/**
 * Created by Eddie on 2017/7/6.
 */
public class FileIOTest {
    @Test
    public void read() throws Exception {
        FileIO.read("conf/spark-api.xml.template");
    }

}