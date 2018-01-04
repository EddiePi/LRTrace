package utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Eddie on 2017/12/19.
 */
public class ShellCommandExecutorTest {
    ShellCommandExecutor executor;
    @Before
    public void setUp() throws Exception {
        //String command = "/home/eddie/hookup/script/move-app-to-queue.sh application_1 alpha";
        //String command = "/home/eddie/hookup/script/test.sh";
        String command = "/home/eddie/spark-2.1.0/bin/spark-submit --properties-file /home/eddie/HiBench/report/wordcount/spark/conf/sparkbench/spark.conf --class com.intel.hibench.sparkbench.micro.ScalaWordCount --master yarn /home/eddie/HiBench/sparkbench/assembly/target/sparkbench-assembly-6.0-SNAPSHOT-dist.jar hdfs://192.168.32.111:9000/HiBench/Wordcount/Input hdfs://192.168.32.111:9000/HiBench/Wordcount/Output";
        String[] envp = {"YARN_CONF_DIR=/home/eddie/hadoop-2.7.3/etc/hadoop"};
        executor = new ShellCommandExecutor(command, envp);
    }

    @Test
    public void execute() throws Exception {
        executor.execute();
        while (true) {
            String result = executor.getOutput();
            System.out.print(result);
            Thread.sleep(500);
        }
    }

}