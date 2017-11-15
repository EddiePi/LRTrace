package Server;

import java.io.IOException;

/**
 * Created by Eddie on 2017/6/14.
 */
public class ShutdownHookThread extends Thread {

    @Override
    public void run() {
        System.out.print("tracer is shutting down.\n");
        Tracer tracer = Tracer.getInstance();
        try{
            tracer.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
