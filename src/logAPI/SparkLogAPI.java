package logAPI;

import Server.TracerConf;

import java.io.File;
import java.io.IOException;

/**
 * Created by Eddie on 2017/7/3.
 */
public class SparkLogAPI extends AbstractLogAPI {
    TracerConf conf = TracerConf.getInstance();
    //public for test
    public String filePath = conf.getStringOrDefault("tracer.log.spark-api.path", "../conf/spark-api.xml");

    public SparkLogAPI() {
        super();
        apiFile = new File(filePath);
        try {
            parseFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
