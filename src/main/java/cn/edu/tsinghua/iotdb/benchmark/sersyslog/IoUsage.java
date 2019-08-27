package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * IO metrics reader.
 */
public enum IoUsage {
    INSTANCE;

    private static Logger log = LoggerFactory.getLogger(IoUsage.class);
    private final int  BEGIN_LINE = 10;
    public enum IOStatistics {
        TPS(1,0),
        MB_READ(2,0),
        MB_WRTN(3,0);

        public int pos;
        public float max;

        IOStatistics(int p,float m){
            this.pos = p;
            this.max = m;
        }
    };

    public Map<String, Float> get(String driveName) {
        float readsPerSec = 0.0f;
        float writesPerSec = 0.0f;
        Map<String, Float> values = new HashMap<>();
        Process process;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -xd 1 2";
            process = r.exec(command);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = input.readLine()) != null) {
                if (line.startsWith(driveName)) {
                    String[] temp = line.split("\\s+");
                    readsPerSec = Float.parseFloat(temp[1]);
                    System.out.println(Arrays.asList(temp));
                    writesPerSec = Float.parseFloat(temp[2]);
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Could not read IO metrics because: " + e.getMessage());
        }
        values.put("readsPerSec", readsPerSec);
        values.put("writesPerSec", writesPerSec);
        return values;
    }

    // TODO legacy
    public ArrayList<Float> get() {
        ArrayList<Float> list = new ArrayList<>();
        float ioUsage = 0.0f;
        float cpuUsage = 0.0f;
        Process process = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -xd 1 2";
            process = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = null;
            int count =  0;
            int flag = 1;

            while((line=in.readLine()) != null) {

                String[] temp = line.split("\\s+");
                if (++count >= 8) {
                    if (temp[0].startsWith("a") && flag == 1) {
                        flag = 0 ;
                    }else  if (flag == 0){
                        cpuUsage = Float.parseFloat(temp[temp.length - 1]);
                        cpuUsage = 1 - cpuUsage/100.0f;
                        flag = 1;
                    } else if(temp.length > 1 && temp[0].startsWith("s")) {
                        float util = Float.parseFloat(temp[temp.length - 1]);
                        //返回设备中利用率最大的
                        ioUsage = (ioUsage > util) ? ioUsage : util;
                    }
                }
            }
            if(ioUsage > 0){
                //log.info("磁盘IO使用率,{}%" , ioUsage);
                ioUsage /= 100.0;
            }
            list.add(cpuUsage);
            list.add(ioUsage);
            in.close();
            process.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return list;
    }

}