package logAPI;

import Server.TracerConf;

import java.io.File;
import java.io.IOException;

/**
 * Created by Eddie on 2017/8/2.
 */
public class YarnLogAPI extends AbstractLogAPI {
    TracerConf conf = TracerConf.getInstance();
    //public for test
    public String filePath = conf.getStringOrDefault("tracer.log.yarn-api.path", "../conf/yarn-api.xml");

    public YarnLogAPI() {
        super();
        apiFile = new File(filePath);
        try {
            parseFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
