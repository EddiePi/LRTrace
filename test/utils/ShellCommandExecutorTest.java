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
        String command = "/home/eddie/hookup/script/move-app-to-queue.sh application_1 alpha";
        //String command = "/home/eddie/hookup/script/test.sh";
        executor = new ShellCommandExecutor(command);
    }

    @Test
    public void execute() throws Exception {
        executor.execute();
        String result = executor.getOutput();
        System.out.print(result);
    }

}