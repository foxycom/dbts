package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

public class Point {
    private long timestamp;
    private String[] values;

    public Point(long timestamp, String value) {
        this.timestamp = timestamp;
        this.values = new String[] {value};
    }

    public Point(long timestamp, String... values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return values[0];
    }

    public String[] getValues() {
        return values;
    }
}
