package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

public class Point {
    private long timestamp;
    private String value;

    public Point(long timestamp, String value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }
}
