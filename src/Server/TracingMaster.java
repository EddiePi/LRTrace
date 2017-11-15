package Server;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;


/**
 * Created by Eddie on 2017/1/23.
 */
public class TracingMaster {
    public static void main(String argv[]) throws Exception {
        PropertyConfigurator.configure("tools-log4j.properties");
        BasicConfigurator.configure();
        Tracer tracer = Tracer.getInstance();
        tracer.init();

        System.out.print("add shutdown hook.\n");
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread());
    }
}
