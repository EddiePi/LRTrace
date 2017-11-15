package Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/4/14.
 */
public class FileReader {
    public static List<String> read(String path) throws IOException {
        File file = new File(path);
        BufferedReader reader = null;
        List<String> stringList = new ArrayList<>();
        try {
            reader = new BufferedReader(new java.io.FileReader(file));
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                stringList.add(tempString);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
            return stringList;
        }
    }
}
