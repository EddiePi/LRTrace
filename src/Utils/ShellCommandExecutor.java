package Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Eddie on 2017/2/15.
 */
public class ShellCommandExecutor {
    String command;
    StringBuffer resultBuffer = null;

    public ShellCommandExecutor(String command) {
        this.command = command;
    }

    public void execute() throws IOException {
        try {
            Process ps = Runtime.getRuntime().exec(command);
            ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            resultBuffer = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                resultBuffer.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getOutput() {
        return (resultBuffer == null) ? "" : resultBuffer.toString();
    }
}
