package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CPU Usage reader.
 */
public enum CpuUsage  {
    INSTANCE;

    public float get() {
        float cpu = 0.0f;
        Process process;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -xc 1 2";
            process = r.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            boolean rightLine = false;
            while ((line = input.readLine()) != null) {
                if (line.startsWith("avg-cpu")) {
                    rightLine = true;
                } else if (rightLine) {
                    String[] values = line.split("\\s+");
                    cpu = Float.parseFloat(values[0]);
                    break;
                }
            }

            input.close();
            process.destroy();
        } catch (IOException e) {
            System.err.println("Could not read CPU usage because: " + e.getMessage());
        }
        return cpu;
    }
}