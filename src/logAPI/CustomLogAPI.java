package logAPI;

import java.io.File;
import java.io.IOException;

/**
 * Created by Eddie on 2017/7/5.
 *
 * Check whether the file exists before create this class
 */
public class CustomLogAPI extends AbstractLogAPI {

    public CustomLogAPI(File file) {
        super();
        apiFile = file;
        try {
            parseFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
