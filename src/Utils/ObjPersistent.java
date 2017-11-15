package Utils;

import java.io.*;
/**
 * Created by Eddie on 2017/4/18.
 */
public class ObjPersistent {
    public static void saveObject(Object o, String path) {
        try {
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(path));
            os.writeObject(o);
            os.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object readObject(String path) {
        Object obj = null;
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(path));
            obj = is.readObject();
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

}
