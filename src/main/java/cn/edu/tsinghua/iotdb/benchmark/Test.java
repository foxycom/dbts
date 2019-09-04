package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.sersyslog.CpuUsage;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.MemUsage;

import java.sql.Timestamp;

public class Test {
    public static void main(String[] args) {
        Timestamp timestamp = new Timestamp(1535559399990L);
        System.out.println(timestamp.toString());

    }
}
