package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * Created by Eddie on 2017/2/15.
 */
public class ShellCommandExecutor {
    String[] commands;
    String command;
    String[] envp;
    boolean multi;
    StringBuffer resultBuffer = null;

    public ShellCommandExecutor(String command) {
        this(command, null);
    }

    public ShellCommandExecutor(String[] commands) {
        this(commands, null);
    }

    public ShellCommandExecutor(String command, String[] envp) {
        this.command = command;
        multi = false;
        this.envp = envp;
    }

    public ShellCommandExecutor(String[] commands, String[] envp) {
        this.commands = commands;
        multi = true;
        this.envp = envp;
    }

    public void execute() throws IOException {
        try {
            Process ps;
            if (multi) {
                ps = Runtime.getRuntime().exec(commands, envp);
            } else {
                ps = Runtime.getRuntime().exec(command, envp);
            }
            int exitCode = ps.waitFor();
            if (exitCode != 0) {
                System.out.printf("shell failed. exit code: %d\n", exitCode);
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            resultBuffer = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                resultBuffer.append(line).append("\n");
            }

            BufferedReader brError = new BufferedReader(new InputStreamReader(ps.getErrorStream(), "gb2312"));
            String errLine;
            while((errLine = brError.readLine()) != null) {
                System.out.println(errLine);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getOutput() {
        String result = "";
        if (resultBuffer != null) {
            result = resultBuffer.toString();
        }
        resultBuffer = null;
        return result;
    }
}
