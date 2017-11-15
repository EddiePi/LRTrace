package logAPI;

import Server.TracerConf;

import java.io.File;
import java.io.IOException;

/**
 * Created by Eddie on 2017/7/3.
 */
public class MapReduceLogAPI extends AbstractLogAPI {
    TracerConf conf = TracerConf.getInstance();
    String filePath = conf.getStringOrDefault("tracer.log.mr-api.path", "../conf/mr-api.xml");

    public MapReduceLogAPI() {
        super();
        apiFile = new File(filePath);
        try {
            parseFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
