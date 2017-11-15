package Utils.FileWatcher;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

/**
 * Created by Eddie on 2017/6/8.
 */
public class WatchDir {
    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;
    private final boolean subDir;

    /**
     * constructor
     *
     * @param file   directory，cannot be files
     * @param subDir
     * @throws Exception
     */
    public WatchDir(File file, boolean subDir, FileActionCallback callback) throws Exception {
        if (!file.isDirectory())
            throw new Exception(file.getAbsolutePath() + "is not a directory!");

        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
        this.subDir = subDir;

        Path dir = Paths.get(file.getAbsolutePath());

        if (subDir) {
            registerAll(dir);
        } else {
            register(dir);
        }
        processEvents(callback);
    }

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    /**
     * specify the observing directory
     *
     * @param dir
     * @throws IOException
     */
    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
        keys.put(key, dir);
    }

    /**
     * include the sub directory
     */
    private void registerAll(final Path start) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * callback function when files are changed
     */
    @SuppressWarnings("rawtypes")
    void processEvents(FileActionCallback callback) {
        for (; ; ) {
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }
            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("unrecognized calculate");
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind kind = event.kind();

                // eventList might be discard
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    continue;
                }

                // change in the directory may be a file or a directory
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);
                File file = child.toFile();
                if (kind.name().equals(FileAction.DELETE.getValue())) {
                    callback.delete(file);
                } else if (kind.name().equals(FileAction.CREATE.getValue())) {
                    callback.create(file);
                } else if (kind.name().equals(FileAction.MODIFY.getValue())) {
                    callback.modify(file);
                } else {
                    continue;
                }

                // if directory is created, and watching recursively, then
                // registerContainerRules it and its sub-directories
                if (subDir && (kind == StandardWatchEventKinds.ENTRY_CREATE)) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            registerAll(child);
                        }
                    } catch (IOException x) {
                        // ignore to keep sample readable
                    }
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                // remove inaccessible directory
                keys.remove(key);
                // stop the watcher when the observing directory is removed.
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

    public void stop() throws IOException {
        watcher.close();
    }
}
