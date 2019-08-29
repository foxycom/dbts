package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mem Usage reader.
 */
public enum MemUsage {
    INSTANCE;

    private final double KB2GB = 1024 * 1024f;

    public Map<String, Float> get() {
        float memUsage = 0.0f;
        float swapUsage = 0.0f;
        Map<String, Float> values = new HashMap<>(2);
        Process process = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "free";
            process = r.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;

            while((line=input.readLine()) != null) {
                line = Usage.parseShellValues(line);
                String[] temp = line.split("\\s+");
                if (temp[0].startsWith("Mem") || temp[0].startsWith("Speicher")) {
                    float memTotal = Float.parseFloat(temp[1]);
                    float memUsed = Float.parseFloat(temp[2]);
                    memUsage = memUsed / memTotal;
                } else if (temp[0].startsWith("Swap") || temp[0].startsWith("Auslagerungsspeicher")) {
                    float swapTotal = Float.parseFloat(temp[1]);
                    float swapUsed = Float.parseFloat(temp[2]);
                    swapUsage = swapUsed / swapTotal;
                }
            }
            input.close();
            process.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        values.put("memUsage", memUsage);
        values.put("swapUsage", swapUsage);
        return values;
    }

}