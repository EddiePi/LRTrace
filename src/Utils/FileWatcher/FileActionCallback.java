package Utils.FileWatcher;

import java.io.File;

/**
 * Created by Eddie on 2017/6/8.
 */
public abstract class FileActionCallback {
    public void delete(File file) {}

    public void modify(File file) {}

    public void create(File file) {}
}
