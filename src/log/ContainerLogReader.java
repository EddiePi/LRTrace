package log;

import logAPI.LogAPICollector;
import logAPI.MessageMark;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Eddie on 2017/6/6.
 */
public class ContainerLogReader {

    File containerDir;
    File[] logFiles;
    List<String> doneFileList = new LinkedList<>();
    Map<String, Integer> readingFileCount = new HashMap<>();
    List<FileReadRunnable> fileReadRunnableList = new ArrayList<>();
    ExecutorService fileReadingThreadPool = Executors.newCachedThreadPool();
    volatile Boolean isChecking = true;
    String containerId;
    Thread checkingThread;
    KafkaLogSender logSender;
    List<MessageMark> containerRules;

    public int timeoutCount;

    public ContainerLogReader(String containerPath) {
        containerRules = LogAPICollector.getInstance().containerRuleMarkList;
        this.containerDir = new File(containerPath);
        timeoutCount = 0;

        containerId = parseContainerId(containerPath);
        checkingThread = new Thread(new ContainerCheckingRunnable());
        checkingThread.start();
    }

    private class ContainerCheckingRunnable implements Runnable {

        @Override
        public void run() {
            while (isChecking) {
                logFiles = containerDir.listFiles();
                for(int i = 0; i < logFiles.length; i++) {
                    try {
                        String filePath = logFiles[i].getCanonicalPath();
                        if(!doneFileList.contains(filePath)
                                && !readingFileCount.containsKey(filePath)) {
                            readingFileCount.put(filePath, 0);
                            FileReadRunnable newFile = new FileReadRunnable(filePath);
                            fileReadRunnableList.add(newFile);
                            fileReadingThreadPool.execute(newFile);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private class FileReadRunnable implements Runnable {

        String filePath;
        //List<String> messageBuffer = new ArrayList<>();
        List<String> messageBuffer = new ArrayList<>();
        BufferedReader bufferedReader = null;
        Boolean isReading = true;


        public FileReadRunnable(String path) {
            this.filePath = path;
            logSender = new KafkaLogSender(containerId);
        }

        @Override
        public void run() {
            try {
                if(bufferedReader == null) {
                    InputStream is = new FileInputStream(filePath);
                    Reader reader = new InputStreamReader(is, "GBK");
                    bufferedReader = new BufferedReader(reader);
                }

                while (isReading) {
                    String line;
                    messageBuffer.clear();
                    while((line = bufferedReader.readLine()) != null) {
                        for(MessageMark mm: containerRules) {
                            Pattern pattern = Pattern.compile(mm.regex);
                            Matcher matcher = pattern.matcher(line);
                            if (matcher.matches()) {
                                messageBuffer.add(line);
                                break;
                            }
                        }
                    }
                    for(String message: messageBuffer) {
                        logSender.send(containerId + " " + message);
                    }
                    Thread.sleep(200);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                Thread.currentThread().interrupt();
            }
        }


        public void destroy() throws IOException {
            isReading = false;
            bufferedReader.close();
        }
    }

    private String parseContainerId(String containerPath) {
        String[] paths = containerPath.split("/");
        return paths[paths.length - 1];
    }



    /**
     * This method will stop all the log readers in this container directory
     */
    public void stop() {
        isChecking = false;
        //checkingThread.interrupt();
        try {
            for (FileReadRunnable runnable : fileReadRunnableList) {
                runnable.destroy();
            }
            fileReadingThreadPool.awaitTermination(2, TimeUnit.SECONDS);
            logSender.send(containerId + " is finished.");
            logSender.close();
            System.out.print("all log readers of container: " + containerDir.getAbsolutePath() + " are stopped.\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
