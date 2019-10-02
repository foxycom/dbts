package de.uni_passau.dbts.benchmark.utils;

import de.uni_passau.dbts.benchmark.enums.Aggregation;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;
import org.apache.commons.lang3.NotImplementedException;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A builder utility for basic SQL statements.
 */
public class SqlBuilder {
    StringBuilder builder;

    /** Creates a builder instance. */
    public SqlBuilder() {
        builder = new StringBuilder();
    }

    /**
     * Resets the builder to an empty state.
     *
     * @return The builder instance.
     */
    public SqlBuilder reset() {
        builder.setLength(0);
        return this;
    }

    /**
     * Adds a SELECT clause to the SQL statement with both aggregated and non-aggregated
     * columns.
     *
     * @param aggregatedColumns List of columns to apply aggregation to.
     * @param plainColumns List of non-aggregated columns.
     * @param aggregationFunc The aggregation function to apply.
     * @return The builder instance.
     */
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
        if (plainColumns != null) {
            plainColumns.forEach(plainColumn -> builder.append(", ").append(plainColumn));
        }
        return this;
    }

    /**
     * Adds a SELECT clause to the SQL statement with a time bucket, columns to
     * aggregate within time buckets, and non-aggregated columns.
     *
     * @param aggregatedColumns List of columns to be aggregated within time buckets.
     * @param plainColumns List of non-aggregated columns.
     * @param aggregationFunc The aggregation function to apply.
     * @param timeBucket Time interval of each time bucket, in ms.
     * @return The builder instance.
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

    /**
     * Adds a SELECT clause to the SQL statement with the specified columns.
     *
     * @param columns List of columns to select.
     * @return The builder instance.
     */
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

    /**
     * Adds a FROM clause with a table name to the SQL statement.
     *
     * @param tableName Name of the table.
     * @return The builder instance.
     */
    public SqlBuilder from(String tableName) {
        builder.append(" FROM ").append(tableName);
        return this;
    }

    /**
     * Adds a WHERE clause to the SQL statement.
     *
     * @return The builder instance.
     */
    public SqlBuilder where() {
        builder.append(" WHERE");
        return this;
    }

    /**
     * Adds an AND clause to the SQL statement.
     *
     * @return The builder instance.
     */
    public SqlBuilder and() {
        builder.append(" AND");
        return this;
    }

    /**
     * Adds an OR clause to the SQL statement.
     *
     * @return The builder instance.
     */
    public SqlBuilder or() {
        builder.append(" OR");
        return this;
    }

    /**
     * Adds a constraint on bike ids to the SQL statement.
     *
     * @param bikes List of bikes.
     * @return The builder instance.
     */
    public SqlBuilder bikes(List<Bike> bikes) {
        builder.append(" (");
        boolean firstIteration = true;
        for (Bike bikeSchema : bikes) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(" OR ");
            }
            builder.append("bike_id = '").append(bikeSchema.getName()).append("'");
        }
        builder.append(")");
        return this;
    }

    /**
     * Adds a precise time constraint to the SQL statement.
     *
     * @param timestamp The timestamp.
     * @return The builder instance.
     */
    public SqlBuilder time(long timestamp) {
        Timestamp ts = new Timestamp(timestamp);
        builder.append(" (time = '").append(ts).append("')");
        return this;
    }

    /**
     * Adds an 'column IS NOT NULL' constraint to the SQL statement.
     *
     * @param column Column name.
     * @return The builder instance.
     */
    public SqlBuilder isNotNull(String column) {
        builder.append(" ").append(column).append(" IS NOT NULL");
        return this;
    }

    /**
     * Adds a time range constraint to the SQL statement.
     *
     * @param query Query parameters.
     * @return The builder instance.
     */
    public SqlBuilder time(Query query) {
        Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
        builder.append(" (time >= '").append(startTimestamp);
        builder.append("' AND time <= '").append(endTimestamp).append("')");
        return this;
    }

    /**
     * Adds a constraint on sensor ids to the SQL statement.
     *
     * @param query Query parameters.
     * @return The builder instance.
     */
    public SqlBuilder sensors(Query query) {
        return sensors(query, false);
    }

    /**
     * Adds a constraint on sensor ids to the SQL statement.
     *
     * @param query Query parameters.
     * @param single If true, only takes the first sensor in the query.
     * @return The builder instance.
     */
    public SqlBuilder sensors(Query query, boolean single) {
        List<Sensor> sensors = new ArrayList<>();

        // TODO fix
        sensors.add(query.getSensor());
        if (single) {
            builder.append(" ").append(Column.SENSOR.getName()).append(" = '")
                    .append(sensors.get(0).getName()).append("'");
            return this;
        }

        builder.append(" (");
        boolean firstIteration = true;
        for (Sensor sensor : sensors) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                builder.append(" OR ");
            }
            builder.append(Column.SENSOR.getName()).append(" = '").append(sensor.getName()).append("'");
        }
        builder.append(")");
        return this;
    }

    /**
     * Adds an constraint on sensor group to the SQL statement.
     *
     * @param sensorGroup Sensor group name
     * @return The builder instance.
     */
    public SqlBuilder sensorGroup(SensorGroup sensorGroup) {
        builder.append(" ").append(Column.SENSOR_GROUP.getName()).append(" = '")
                .append(sensorGroup.getName()).append("'");
        return this;
    }

    /**
     * Adds a value constraint to the SQL statement. Should only be used for narrow tables.
     *
     * @param op Operand to apply.
     * @param otherValue Other value to compare.
     * @return The builder instance.
     */
    public SqlBuilder value(Op op, double otherValue) {
        builder.append(" value ").append(op.sign()).append(" ").append(otherValue);
        return this;
    }

    /**
     * Adds a value constraint to the SQL statement. Should only be used for wide tables.
     *
     * @param column Column name.
     * @param op Operand to apply.
     * @param otherValue Other value to compare.
     * @return The builder instance.
     */
    public SqlBuilder value(String column, Op op, double otherValue) {
        builder.append(" ").append(column).append(" ").append(op.sign()).append(" ").append(otherValue);
        return this;
    }

    /**
     * Adds a GROUP BY clause to the SQL statement.
     *
     * @param groupBy List of columns to group by.
     * @return The builder instance.
     */
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

    /**
     * Adds an ORDER BY clause to the SQL statement.
     *
     * @param column Column name.
     * @param order Sorting order.
     * @return The builder instance.
     */
    public SqlBuilder orderBy(String column, Order order) {
        return orderBy(Collections.singletonList(column), order);
    }

    /**
     * Adds an ORDER BY clause to the SQL statement.
     *
     * @param columns List of columns to sort by.
     * @param order Sorting order.
     * @return The builder instance.
     */
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

    /**
     * Adds a LIMIT clause to the SQL statement.
     *
     * @param limit Limit value.
     * @return The builder instance.
     */
    public SqlBuilder limit(int limit) {
        builder.append(" LIMIT ").append(limit);
        return this;
    }

    /**
     * Adds a GROUP BY clause to the SQL statement.
     *
     * @param column Column name.
     * @return The builder instance.
     */
    public SqlBuilder groupBy(Column column) {
        builder.append(" GROUP BY ").append(column.getName());
        return this;
    }

    /**
     * Adds a GROUP BY clause to the SQL statement.
     *
     * @param time Time value.
     * @return The builder instance.
     */
    public SqlBuilder groupBy(long time) {
        throw new NotImplementedException("");
    }

    /**
     * Returns an SQL statement.
     *
     * @return SQL statement.
     */
    public String build() {
        builder.append(";");
        return builder.toString();
    }

    /**
     * List of operands.
     */
    public enum Op {
        EQUALS("="),
        UNEQUALS("<>"),
        EQUALS_LESS("<="),
        EQUALS_GREATER(">="),
        LESS("<"),
        GREATER(">");

        private String sign;

        /**
         * Initializes an operand.
         * @param sign
         */
        Op(String sign) {
            this.sign = sign;
        }

        /**
         * Returns the sign of the operand.
         *
         * @return The sign.
         */
        public String sign() {
            return this.sign;
        }
    }

    /**
     * Default column list.
     */
    public enum Column {
        VALUE("value"),
        BIKE("bike_id"),
        SENSOR("sensor_id"),
        TIME("time"),
        SENSOR_GROUP("sensor_group_id"),
        OWNER_NAME("owner_name");

        /** Name of the column. */
        private String name;

        /**
         * Initializes a column.
         *
         * @param name Name of the column.
         */
        Column(String name) {
            this.name = name;
        }

        /**
         * Returns the name of the column.
         *
         * @return The name of the column.
         */
        public String getName() {
            return this.name;
        }
    }

    /**
     * Sorting order.
     */
    public enum Order {
        ASC,
        DESC
    }

}
