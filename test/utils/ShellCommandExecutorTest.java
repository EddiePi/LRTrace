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
        String command = "/home/eddie/hadoop-2.7.3/bin/yarn queue -status alpha";
        executor = new ShellCommandExecutor(command);
    }

    @Test
    public void execute() throws Exception {
        executor.execute();
        String result = executor.getOutput();
        String[] lines = result.split("\\n");
        System.out.print(lines[4].trim());
    }

}