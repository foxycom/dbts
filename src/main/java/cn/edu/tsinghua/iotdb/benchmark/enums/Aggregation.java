package cn.edu.tsinghua.iotdb.benchmark.enums;

public enum Aggregation {
    COUNT,
    AVG,
    SUM,
    MAX,
    MIN,
    LAST,
    FIRST,
    TIME_BUCKET,
    MEAN;

    public String build(String column) {
        switch (this) {
            case AVG:
            case MAX:
            case MIN:
            case LAST:
            case FIRST:
            case SUM:
            case MEAN:
            case COUNT:
                return this.name() + "(" + column + ")";
            case TIME_BUCKET:
                throw new IllegalArgumentException("Please use the overloaded method for time buckets.");
            default:
                throw new IllegalStateException("Aggregation operation not supported.");
        }
    }

    public String build(String column, long timeBucket) {
        if (this == TIME_BUCKET) {
            return this.name().toLowerCase() + "(interval '" + timeBucket + " ms', " + column + ") as time_bucket";
        } else {
            return build(column);
        }
    }
}
