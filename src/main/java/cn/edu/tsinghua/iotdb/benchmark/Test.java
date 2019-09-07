package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.sersyslog.CpuUsage;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.MemUsage;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;

public class Test {
    public static void main(String[] args) {
        Instant instant = Timestamp.valueOf("2018-08-30 01:06:39.98").toInstant();
        System.out.println(instant.toEpochMilli());
    }
}
