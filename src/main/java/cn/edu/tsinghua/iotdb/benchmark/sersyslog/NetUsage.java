package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Net metrics reader.
 */
public enum NetUsage {
    INSTANCE;

    private final static float TotalBandwidth = 1000;   //网口带宽,Mbps,假设是前兆以太网

    /**
     * Reads inet throughput in KB/s with bwm-ng, which has following output:
     *
     *     enp0s25
     *  KB/s in  KB/s out
     *     0.20      0.14
     *
     * @param iface
     * @return
     */
    public Map<String, Float> get(String iface) {
        float recvPerSec = 0.0f;
        float transPerSec = 0.0f;
        Map<String, Float> values = new HashMap<>();
        Process process;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "ifstat -i " + iface + " 1 1";
            process = r.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            boolean rightSection = false;
            while ((line = input.readLine()) != null) {
                if (line.contains(iface)) {
                    rightSection = true;
                } else if (rightSection) {
                    if (line.contains("KB/s")) {

                    } else {
                        line = Usage.parseShellValues(line);
                        String[] temp = line.split("\\s+");

                        recvPerSec = Float.parseFloat(temp[0]);
                        transPerSec = Float.parseFloat(temp[1]);
                        break;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Could not read net metrics of " + iface + " because: " + e.getMessage());
        }
        values.put("recvPerSec", recvPerSec);
        values.put("transPerSec", transPerSec);
        return values;
    }

}