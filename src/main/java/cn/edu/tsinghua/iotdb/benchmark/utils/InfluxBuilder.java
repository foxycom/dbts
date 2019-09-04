package cn.edu.tsinghua.iotdb.benchmark.utils;

import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;

public class InfluxBuilder extends SqlBuilder {
    private static final String TRAILING_NANOS = "000000";

    @Override
    public InfluxBuilder time(RangeQuery rangeQuery) {
        builder.append(" (time >= ").append(rangeQuery.getStartTimestamp()).append(TRAILING_NANOS)
        .append(" AND time <= ").append(rangeQuery.getEndTimestamp()).append(TRAILING_NANOS).append(")");
        return this;
    }

    @Override
    public InfluxBuilder time(PreciseQuery preciseQuery) {
        builder.append(" time = ").append(preciseQuery.getTimestamp()).append(TRAILING_NANOS);
        return this;
    }

    @Override
    public InfluxBuilder groupBy(long time) {
        builder.append(" GROUP BY time(").append(time).append("ms)");
        return this;
    }
}
