package Utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/7/6.
 */
public class FileReaderTest {
    @Test
    public void read() throws Exception {
        FileReader.read("conf/spark-api.xml.template");
    }

}