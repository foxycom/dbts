package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * CPU Usage reader.
 */
public enum CpuUsage  {
    INSTANCE;

    /**
     * Reads the CPU usage of %user with iostat. Sample output:
     *
     * Linux 5.0.0-21-generic (tim-ba-client) 	08/27/2019 	_x86_64_	(4 CPU)
     *
     * avg-cpu:  %user   %nice %system %iowait  %steal   %idle
     *            0.06    0.00    0.01    0.18    0.00   99.75
     *
     * @return The average CPU usage;
     */
    public float get() {
        float cpu = 0.0f;
        Process process;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -c 1 2";
            process = r.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            int section = 0;
            while ((line = input.readLine()) != null) {
                if (line.contains("avg-cpu")) {
                    section++;
                } else if (section == 2) {
                    String[] values = line.split("\\s+");
                    cpu = Float.parseFloat(values[1]);
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