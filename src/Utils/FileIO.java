package Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Eddie on 2017/4/14.
 */
public class FileIO {
    public static List<String> read(String path) throws IOException {
        File file = new File(path);
        BufferedReader reader = null;
        List<String> stringList = new ArrayList<>();

        reader = new BufferedReader(new java.io.FileReader(file));
        String tempString;
        while ((tempString = reader.readLine()) != null) {
            stringList.add(tempString);
        }
        reader.close();

        return stringList;
    }

    public static void write(String path, String content) throws IOException {
        BufferedWriter writer = null;
        File file = new File(path);
        writer = new BufferedWriter(new java.io.FileWriter(file));
        writer.write(content);
        writer.close();

    }
}
