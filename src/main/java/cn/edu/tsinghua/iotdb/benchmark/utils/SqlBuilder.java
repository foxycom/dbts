package cn.edu.tsinghua.iotdb.benchmark.utils;

import cn.edu.tsinghua.iotdb.benchmark.enums.Aggregation;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

public class SqlBuilder {
    private StringBuilder builder;

    public SqlBuilder() {
        builder = new StringBuilder();
    }

    public SqlBuilder reset() {
        builder.setLength(0);
        return this;
    }

    public SqlBuilder select(List<String> aggregatedColumns, List<String> plainColumns, Aggregation aggregationFunc) {
        // return select(aggregatedColumns, plainColumns, aggregationFunc);
        if (aggregatedColumns.isEmpty()) {
            throw new IllegalArgumentException("There should be at least one column to aggregate.");
        }

        builder.append("SELECT ");
        boolean firstIteration = true;
        for (String aggregatedColumn : aggregatedColumns) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(", ");
            }

            builder.append(aggregationFunc.build(aggregatedColumn));
        }
        plainColumns.forEach(plainColumn -> builder.append(", ").append(plainColumn));
        return this;
    }

    /**
     * Time bucket aggregation.
     *
     * @param aggregatedColumns
     * @param plainColumns
     * @param aggregationFunc
     * @param timeBucket
     * @return
     */
    public SqlBuilder select(List<String> aggregatedColumns, List<String> plainColumns, Aggregation aggregationFunc,
                             long timeBucket) {
        if (aggregatedColumns.isEmpty()) {
            throw new IllegalArgumentException("There should be at least one column to aggregate.");
        } else if (aggregationFunc == Aggregation.TIME_BUCKET) {
            throw new IllegalArgumentException("Can't have a second time_bucket function in the same query.");
        }

        builder.append("SELECT ").append(Aggregation.TIME_BUCKET.build("time", timeBucket));
        aggregatedColumns.forEach(aggregatedColumn -> builder.append(", ").append(aggregationFunc.build(aggregatedColumn)));
        plainColumns.forEach(plainColumn -> builder.append(", ").append(plainColumn));
        return this;
    }

    public SqlBuilder select(List<String> columns) {
        builder.append("SELECT ");
        boolean firstIteration = true;
        for (String column : columns) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(", ");
            }
            builder.append(column);
        }
        return this;
    }

    public SqlBuilder from(String tableName) {
        builder.append(" FROM ").append(tableName);
        return this;
    }

    public SqlBuilder where() {
        builder.append(" WHERE");
        return this;
    }

    public SqlBuilder and() {
        builder.append(" AND");
        return this;
    }

    public SqlBuilder or() {
        builder.append(" OR");
        return this;
    }

    public SqlBuilder bikes(List<DeviceSchema> bikeSchemas) {
        builder.append(" (");
        boolean firstIteration = true;
        for (DeviceSchema bikeSchema : bikeSchemas) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(" OR ");
            }
            builder.append("bike_id = '").append(bikeSchema.getDevice()).append("'");
        }
        builder.append(")");
        return this;
    }

    public SqlBuilder time(PreciseQuery preciseQuery) {
        Timestamp timestamp = new Timestamp(preciseQuery.getTimestamp());
        builder.append(" (time = '").append(timestamp).append("')");
        return this;
    }

    public SqlBuilder isNotNull(String column) {
        builder.append(" ").append(column).append(" IS NOT NULL");
        return this;
    }

    public SqlBuilder time(RangeQuery rangeQuery) {
        Timestamp startTimestamp = new Timestamp(rangeQuery.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(rangeQuery.getEndTimestamp());
        builder.append(" (time >= '").append(startTimestamp);
        builder.append("' AND time <= '").append(endTimestamp).append("')");
        return this;
    }

    public SqlBuilder sensors(RangeQuery rangeQuery) {
        List<Sensor> sensors = rangeQuery.getSensorGroup().getSensors();
        builder.append(" (");
        boolean firstIteration = true;
        for (Sensor sensor : sensors) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(" OR ");
            }
            builder.append("sensor_id = '").append(sensor.getName()).append("'");
        }
        builder.append(")");
        return this;
    }

    /**
     * for narrow table only
     * @param op
     * @param otherValue
     * @return
     */
    public SqlBuilder value(Op op, double otherValue) {
        builder.append(" value ").append(op.sign()).append(" ").append(otherValue);
        return this;
    }

    public SqlBuilder value(String column, Op op, double otherValue) {
        builder.append(" ").append(column).append(" ").append(op.sign()).append(" ").append(otherValue);
        return this;
    }

    public SqlBuilder groupBy(List<String> groupBy) {
        builder.append(" GROUP BY ");
        boolean firstIteration = true;
        for (String group : groupBy) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(", ");
            }
            builder.append(group);
        }
        return this;
    }

    public SqlBuilder orderBy(String column, Order order) {
        return orderBy(Collections.singletonList(column), order);
    }

    public SqlBuilder orderBy(List<String> columns, Order order) {
        builder.append(" ORDER BY ");
        boolean firstIteration = true;
        for (String column : columns) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(", ");
            }
            builder.append(column).append(" ").append(order.name());
        }
        return this;
    }

    public SqlBuilder limit(int limit) {
        builder.append(" LIMIT ").append(limit);
        return this;
    }

    public SqlBuilder groupBy(Column column) {
        builder.append(" GROUP BY ").append(column.getName());
        return this;
    }

    public String build() {
        builder.append(";");
        return builder.toString();
    }

    public enum Op {
        EQUALS("="),
        UNEQUALS("<>"),
        EQUALS_LESS("<="),
        EQUALS_GREATER(">="),
        LESS("<"),
        GREATER(">");

        private String sign;

        Op(String sign) {
            this.sign = sign;
        }

        public String sign() {
            return this.sign;
        }
    }

    public enum Column {
        VALUE("value"),
        BIKE("bike_id"),
        SENSOR("sensor_id"),
        TIME("time");

        private String name;

        Column(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    public enum Order {
        ASC,
        DESC
    }

}
