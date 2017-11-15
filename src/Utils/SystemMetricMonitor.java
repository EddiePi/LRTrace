package Utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by Eddie on 2017/9/15.
 */
public class SystemMetricMonitor {

    Runnable monitorRunnable = new MonitorRunnable();
    Thread monitorThread = new Thread(monitorRunnable);


    Boolean isRunning = true;
    private class MonitorRunnable implements Runnable {
        FileReader reader;
        List<String> contents;
        @Override
        public void run() {
            try {
            while(isRunning) {

                    contents = FileReader.read("/proc/meminfo");
                    for (String line: contents) {
                        if (line.contains("SwapFree")) {
                            Date now = new Date();
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String date = format.format(now);
                            System.out.print(date + "\t" + line + "\n");
                            break;
                        }
                    }
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void start() {
        monitorThread.start();
    }

    public void stop() {
        isRunning = false;
    }
}
