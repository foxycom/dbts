package cn.edu.tsinghua.iotdb.benchmark.tsdb.citus;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.Sensors;
import cn.edu.tsinghua.iotdb.benchmark.utils.SqlBuilder;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Point;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GeoPoint;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class Citus implements IDatabase {

    private static final int B2GB = 1024 * 1024 * 1024;
    private Connection connection;
    private static String tableName;
    private static Config config;
    private static final Logger LOGGER = LoggerFactory.getLogger(Citus.class);
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s CASCADE;";
    private long initialDbSize;
    private SqlBuilder sqlBuilder;
    private Faker nameFaker;

    /**
     * Initializes an instance of the database controller.
     */
    public Citus() {
        sqlBuilder = new SqlBuilder();
        config = ConfigParser.INSTANCE.config();
        tableName = config.DB_NAME;
        nameFaker = new Faker();
    }

    /**
     * Initializes a connection to the database.
     *
     * @throws TsdbException If a connection could not be established.
     */
    @Override
    public void init() throws TsdbException {
        try {
            Class.forName(Constants.POSTGRESQL_JDBC_NAME);
            connection = DriverManager.getConnection(
                    String.format(Constants.POSTGRESQL_URL, config.HOST, config.PORT, config.DB_NAME),
                    Constants.POSTGRESQL_USER,
                    Constants.POSTGRESQL_PASSWD
            );
        } catch (Exception e) {
            LOGGER.error("Initialize TimescaleDB failed because ", e);
            throw new TsdbException(e);
        }
    }

    /*
     * Returns the size of a given database in bytes.
     */
    private long getInitialSize() {
        String sql = "";
        long initialSize = 0;
        try (Statement statement = connection.createStatement()) {
            sql = "SELECT pg_database_size('%s') as initial_db_size;";
            sql = String.format(sql, config.DB_NAME);
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                initialSize = rs.getLong("initial_db_size");
            }
        } catch (SQLException e) {
            LOGGER.warn("Could not query the initial DB size of TimescaleDB with: {}", sql);
        }
        return initialSize;
    }

    /**
     * Erases the data from the database by dropping benchmark tables.
     *
     * @throws TsdbException If an error occurs while cleaning up.
     */
    @Override
    public void cleanup() throws TsdbException {
        //delete old data
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.addBatch(String.format(DROP_TABLE, "bikes"));
            statement.addBatch(String.format(DROP_TABLE, "grid"));
            statement.addBatch(String.format(DROP_TABLE, tableName));
            statement.executeBatch();
            connection.commit();

            initialDbSize = getInitialSize();

            // wait for deletion complete
            LOGGER.info("Waiting {}ms for old data deletion.", config.ERASE_WAIT_TIME);
            Thread.sleep(config.ERASE_WAIT_TIME);
        } catch (SQLException e) {
            LOGGER.warn("delete old data table {} failed, because: {}", tableName, e.getMessage());
            LOGGER.warn(e.getNextException().getMessage());

            if (!e.getMessage().contains("does not exist")) {
                throw new TsdbException(e);
            }
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Closes the connection to the database.
     *
     * @throws TsdbException If connection could not be closed.
     */
    @Override
    public void close() throws TsdbException {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close TimeScaleDB connection because: {}", e.getMessage());
            throw new TsdbException(e);
        }
    }

    /**
     * Maps the data schema concepts:
     * <ul>
     * <li>Sensor group name -> table name</li>
     * <li>Bike name -> a field in table</li>
     * <li>Sensor name -> a field in table</li>
     * <li>Value(s) -> fields in table</li>
     * </ul>
     * <p> Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables</p>
     * -- We start by creating a regular PG table:
     * <p><code>
     * CREATE TABLE gps_benchmark (time TIMESTAMPTZ NOT NULL, bike_id VARCHAR(20) NOT NULL,
     *    sensor_id VARCHAR(20) NOT NULL, value REAL NULL);
     * </code></p>
     * -- This creates a hypertable that is partitioned by time using the values in the `time` column.
     * <p><code>SELECT create_hypertable('gps_benchmark', 'time', chunk_time_interval => interval '1 h');</code></p>
     */
    @Override
    public void registerSchema(List<Bike> schemaList) throws TsdbException {
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            createGridFunction(statement);

            // Creates bikes' relational data table.
            statement.addBatch(
                    "CREATE TABLE IF NOT EXISTS bikes (bike_id VARCHAR PRIMARY KEY, owner_name VARCHAR(100) NOT NULL);"
            );
            statement.addBatch(
                    "CREATE INDEX ON bikes (bike_id);"
            );

            // Inserts all bikes.
            statement.addBatch(getInsertBikesSql(schemaList));

            statement.addBatch(getCreateGridTableSql());

            statement.addBatch(getCreateTableSql());
            statement.executeBatch();
            connection.commit();

            createIndexes(statement);
            distributeTables(statement);
        } catch (SQLException e) {
            LOGGER.error("Can't create PG table because: {}", e.getMessage());
            System.out.println(e.getNextException());
            throw new TsdbException(e);
        }
    }

    /**
     * Returns the size of the benchmarked database in GB.
     *
     * @return The size of the benchmarked database, i. e., 'test'.
     * @throws TsdbException If an error occurs while executing a query.
     */
    @Override
    public float getSize() throws TsdbException {
        float resultInGB = 0.0f;
        try (Statement statement = connection.createStatement()) {
            String selectSizeSql = String.format("SELECT pg_database_size('%s') as db_size;", config.DB_NAME);
            ResultSet rs = statement.executeQuery(selectSizeSql);
            if (rs.next()) {
                long resultInB = rs.getLong("db_size");
                resultInB -= initialDbSize;
                resultInGB = (float) resultInB / B2GB;
            }
            return resultInGB;
        } catch (SQLException e) {
            LOGGER.error("Could not read the size of the data because: {}", e.getMessage());
            throw new TsdbException(e);
        }
    }

    /*
     * Creates and executes SQL queries to create index structures on tables of metrics.
     */
    private void createIndexes(Statement statement) throws SQLException {
        connection.setAutoCommit(false);
        String indexOnGridTableSql = "CREATE INDEX ON grid USING GIST(geom);";
        statement.addBatch(indexOnGridTableSql);

        SensorGroup gpsSensorGroup = Sensors.groupOfType("gps");
        for (Sensor sensor : gpsSensorGroup.getSensors()) {
            String createGeoIndexSql = "CREATE INDEX ON " + tableName + " USING GIST (" + sensor.getName() + ")";
            statement.addBatch(createGeoIndexSql);
        }

        statement.executeBatch();
        connection.commit();
    }

    private void distributeTables(Statement statement) throws SQLException {
        connection.setAutoCommit(false);
        statement.execute(
                "SELECT create_distributed_table('bikes', 'bike_id');"
        );
        statement.execute(
                "SELECT create_distributed_table('test', 'bike_id')"
        );
        statement.execute(
                "CREATE TABLE test_2018_08_00 PARTITION OF test FOR VALUES FROM ('2018-08-30') TO ('2018-08-31');"
        );
        connection.commit();
    }

    /*
     * Registers a function in SQL, which generates a grid for a heat map.
     */
    private void createGridFunction(Statement statement) throws SQLException {
        String sql = "CREATE OR REPLACE FUNCTION ST_CreateGrid(\n" +
                "nrow integer, ncol integer,\n" +
                "xsize float8, ysize float8,\n" +
                "x0 float8 DEFAULT 0, y0 float8 DEFAULT 0,\n" +
                "OUT \"row\" integer, OUT col integer,\n" +
                "OUT geom geometry)\n" +
                "RETURNS SETOF record AS\n" +
                "$$\n" +
                "SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom\n" +
                "FROM generate_series(0, $1 - 1) AS i,\n" +
                "generate_series(0, $2 - 1) AS j,\n" +
                "(\n" +
                "SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell\n" +
                ") AS foo;\n" +
                "$$ LANGUAGE sql IMMUTABLE STRICT;";
        statement.executeUpdate(sql);
    }

    private String getCreateGridTableSql() {
        String sql = "create table grid as \n" +
                "select (st_dump(map.geom)).geom from (\n" +
                "\tselect st_setsrid(st_collect(grid.geom),4326) as geom \n" +
                "\tfrom ST_CreateGrid(40, 90, 0.0006670, 0.0006670, %f, %f) as grid\n" +
                ") as map;";
        sql = String.format(
                Locale.US,
                sql,
                Constants.GRID_START_POINT.getLongitude(),
                Constants.GRID_START_POINT.getLatitude());
        return sql;
    }

    /**
     * Writes the given batch of data points into the database and measures the time the database takes to store the
     * points.
     *
     * @param batch The batch of data points.
     * @return The status of the execution.
     */
    @Override
    public Status insertOneBatch(Batch batch) {
        long startTime;
        long endTime;
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);

            String insertBatchSql = getInsertOneWideBatchSql(batch);
            statement.addBatch(insertBatchSql);
            startTime = System.nanoTime();
            statement.executeBatch();
            connection.commit();
            endTime = System.nanoTime();
            return new Status(true, endTime - startTime);
        } catch (SQLException e) {
            System.out.println(e.getNextException().getMessage());
            return new Status(false, 0, e, e.toString());
        }
    }

    /**
     * Selects data points, which are stored with a given timestamp.
     *
     * NARROW_TABLE:
     * <p><code>
     *  SELECT value, time, bike_id FROM emg_benchmark WHERE (bike_id = 'bike_10') AND (time = '2018-08-29 18:00:00.0');
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_8') AND (time = '2018-08-29 18:00:00.0');
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status precisePoint(Query query) {
        Sensor sensor = query.getSensor();
        long timestamp = query.getStartTimestamp();

        List<String> columns = Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName(),
                sensor.getName());
        sqlBuilder = sqlBuilder.reset().select(columns).from(tableName).where().bikes(query.getBikes())
                .and().time(timestamp);
        return executeQuery(sqlBuilder.build());
    }

    /**
     * Selects the last known GPS position of bikes.
     *
     * NARROW_TABLE:
     * <p><code>
     *  SELECT value, bike_id, time, sensor_id FROM gps_benchmark WHERE (bike_id = 'bike_5') ORDER BY time DESC LIMIT 1;
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  select data.time, data.bike_id, b.owner_name, data.s_12 from bikes b inner join lateral (
     * 	select * from test t where t.bike_id = b.bike_id and s_12 is not null
     * 	order by time desc limit 1
     *  ) as data on true;
     * </code></p>
     *
     * @param query The query parameters query.
     * @return The status of the execution.
     */
    @Override
    public Status lastKnownPosition(Query query) {
        Sensor gpsSensor = query.getGpsSensor();
        String sql = "";

        sql = "with location as (\n" +
                "\tselect distinct on(bike_id) bike_id, %s, time \n" +
                "\tfrom %s t\n" +
                "\twhere %s is not null\n" +
                "\tgroup by bike_id, time, %s\n" +
                "\torder by bike_id, time desc\n" +
                ") select l.time, b.bike_id, b.owner_name, l.%s \n" +
                "from location l, bikes b where b.bike_id = l.bike_id;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
                gpsSensor.getName(),
                gpsSensor.getName(),
                gpsSensor.getName()
        );

        return executeQuery(sql);
    }

    /**
     * Performs a scan of gps data points in a given time range for specified number of bikes.
     *
     * NARROW_TABLE:
     * <p><code>
     *  SELECT time, bike_id, value FROM gps_benchmark WHERE (bike_id = 'bike_10') AND (time >= '2018-08-29 18:00:00.0'
     *  AND time <= '2018-08-29 19:00:00.0');
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  select data.bike_id, b.owner_name, data.location from bikes b inner join lateral (
     * 	select s_12 as location, bike_id from test t
     * 	where t.bike_id = b.bike_id
     * 	and bike_id = 'bike_0'
     * 	and s_12 is not null
     * 	and time >= '2018-08-29 18:00:00.0'
     * 	and time <= '2018-08-29 19:00:00.0'
     *  ) as data on true;
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status gpsPathScan(Query query) {
        Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
        Sensor gpsSensor = query.getGpsSensor();
        Bike bike = query.getBikes().get(0);
        List<String> columns = new ArrayList<>(Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
        String sql = "";

        sql = "select data.bike_id, b.owner_name, data.location from bikes b inner join lateral (\n" +
                "\tselect %s as location, bike_id from test t\n" +
                "\twhere t.bike_id = b.bike_id \n" +
                "\tand bike_id = '%s' \n" +
                "\tand %s is not null\n" +
                "\tand time >= '%s' \n" +
                "\tand time <= '%s'\n" +
                ") as data on true;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                bike.getName(),
                gpsSensor.getName(),
                startTimestamp,
                endTimestamp
        );
        return executeQuery(sql);
    }

    /**
     * Identifies active trips, when current value is above a threshold.
     *
     * NARROW_TABLE:
     * <p><code>
     *  WITH trip (second, current_value) AS (
     *   SELECT time_bucket('1 second', time) AS second, value(AVG)
     *   FROM emg_benchmark WHERE bike_id = 'bike_0' and time > '2018-08-29 18:00:00.0' and time < '2018-08-29 19:00:00.0'
     *   GROUP BY second HAVING AVG(value) > 3.000000
     *  ) SELECT g.time, t.current_value, g.value as location FROM gps_benchmark g INNER JOIN trip t ON g.time = t.second
     *  WHERE g.bike_id = 'bike_0';
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  select data.second, data.bike_id, b.owner_name, data.s_12 from bikes b inner join lateral (
     * 	select time_bucket(interval '1 s', time) as second, bike_id, s_12
     * 	from test t
     * 	where t.bike_id = b.bike_id
     * 	and t.bike_id = 'bike_2'
     * 	and s_12 is not null
     * 	and time >= '2018-08-29 18:00:00.0'
     * 	and time < '2018-08-29 19:00:00.0'
     * 	group by second, bike_id, s_12
     * 	having avg(s_40) >= 3.000000
     *  ) as data on true
     *  order by data.second asc, data.bike_id;
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status identifyTrips(Query query) {
        Bike bike = query.getBikes().get(0);
        Timestamp startTimestamp = new Timestamp(Constants.START_TIMESTAMP);
        Timestamp endTimestamp = new Timestamp(Constants.START_TIMESTAMP + config.QUERY_INTERVAL);
        Sensor sensor = query.getSensor();
        Sensor gpsSensor = query.getGpsSensor();
        String sql = "";

        sql = "select data.second, data.bike_id, b.owner_name, data.%s from bikes b inner join lateral (\n" +
                "\tselect date_trunc('second', time) as second, bike_id, %s\n" +
                "\tfrom test t \n" +
                "\twhere t.bike_id = b.bike_id \n" +
                "\tand t.bike_id = '%s'\n" +
                "\tand %s is not null \n" +
                "\tand time >= '%s' \n" +
                "\tand time < '%s'\n" +
                "\tgroup by second, bike_id, %s\n" +
                "\thaving avg(%s) >= %f\n" +
                ") as data on true\n" +
                "order by data.second asc, data.bike_id;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                gpsSensor.getName(),
                bike.getName(),
                gpsSensor.getName(),
                startTimestamp,
                endTimestamp,
                gpsSensor.getName(),
                sensor.getName(),
                query.getThreshold()
        );

        return executeQuery(sql);
    }

    /**
     * Computes the distance driven by a bike in the given time range identified by start and end timestamps where current
     * exceeds some value.
     *
     * NARROW_TABLE:
     * <p><code>
     *  with trip_begin as (
     * 	select time_bucket(interval '1 s', time) as second, bike_id from emg_benchmark where value > 3.000000 and bike_id = 'bike_10' and time > '2018-08-29 18:00:00.0' and time < '2018-08-29 19:00:00.0'group by second, bike_id order by second asc limit 1
     *  ), trip_end as (
     *  	select time_bucket(interval '1 s', time) as second, bike_id from emg_benchmark where value > 3.000000 and bike_id = 'bike_10' and time > '2018-08-29 18:00:00.0'and time < '2018-08-29 19:00:00.0' group by second, bike_id order by second desc limit 1
     *  )
     *  select st_length(st_makeline(g.value::geometry)::geography, false) from gps_benchmark g, trip_begin b, trip_end e where g.bike_id = 'bike_10'
     *  and g.time > b.second and g.time < e.second group by g.bike_id;
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  select b.bike_id, b.owner_name, st_length(st_makeline(s_12::geometry)::geography, false)
     *  from bikes b inner join lateral (
     * 	select time_bucket(interval '1 s', time) as second, s_12 from test t
     * 	where t.bike_id = b.bike_id and time >= '2018-08-29 18:00:00.0' and time <= '2018-08-29 19:00:00.0'
     * 	group by second, s_12 having avg(s_40) > 3.000000
     *  ) as data on true
     *  group by b.bike_id, b.owner_name;
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status distanceDriven(Query query) {
        Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
        String sql = "";
        Bike bike = query.getBikes().get(0);
        Sensor sensor = query.getSensor();
        Sensor gpsSensor = query.getGpsSensor();

        sql = "with data as (\n" +
                "\tselect date_trunc('second', time) as second, bike_id, %s \n" +
                "\tfrom %s t\n" +
                "\twhere bike_id = '%s' \n" +
                "\tand time >= '%s' and time <= '%s'\n" +
                "\tgroup by second, bike_id, %s\n" +
                "\thaving avg(%s) > %f and %s is not null\n" +
                "\torder by second\n" +
                ")\n" +
                "select d.bike_id, st_length(st_makeline(d.%s::geometry)::geography) \n" +
                "from data d\n" +
                "group by d.bike_id;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
                bike.getName(),
                startTimestamp,
                endTimestamp,
                gpsSensor.getName(),
                sensor.getName(),
                query.getThreshold(),
                gpsSensor.getName(),
                gpsSensor.getName()
        );
        return executeQuery(sql);
    }

    /**
     * Computes road segments with high average sensor measurements values, e.g., accelerometer, identifying spots
     * where riders often use brakes.
     *
     * NARROW_TABLE:
     * <p><code>
     *  WITH trip AS (
     * 	SELECT time_bucket(interval '1 s', time) AS second, bike_id, AVG(value) AS value FROM emg_benchmark
     * 	WHERE value > 3.000000 AND bike_id = 'bike_10'
     * 	GROUP BY second, bike_id
     *  )
     *  SELECT time_bucket(interval '300000 ms', g.time) AS half_minute, AVG(t.value), t.bike_id, st_makeline(g.value::geometry) FROM gps_benchmark g, trip t
     *  WHERE g.bike_id = 'bike_10' AND g.time = t.second AND g.time > '2018-08-29 18:00:00.0' AND g.time < '2018-08-29 19:00:00.0'
     *  GROUP BY half_minute, t.bike_id;
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  select time_bucket(interval '10 s', time) as five_seconds, st_makeline(s_12::geometry)
     *  from test t where time >= '2018-08-30 02:00:00.0' and time <= '2018-08-30 03:00:00.0'
     *  group by five_seconds, bike_id having avg(s_40) > 3.000000;
     * </code></p>
     * @param query
     * @return
     */
    @Override
    public Status offlineBikes(Query query) {
        String sql = "";
        Sensor sensor = query.getSensor();
        Sensor gpsSensor = query.getGpsSensor();
        Bike bike = query.getBikes().get(0);

        Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());

        sql = "select time_bucket(interval '10 s', time) as five_seconds, st_makeline(%s::geometry) \n" +
                "from %s t where time >= '%s' and time <= '%s' group by five_seconds having avg(%s) > %f;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
                startTimestamp,
                endTimestamp,
                sensor.getName(),
                query.getThreshold()
        );
        return executeQuery(sql);
    }

    /**
     *
     * TODO probably include also gps coordinates or path segment
     * Scans metrics of a sensor in a specific time interval and selects those which have a certain value.
     *
     * NARROW_TABLE:
     * <p><code>
     *  SELECT time, bike_id, value FROM emg_benchmark WHERE value > 3.0 AND (bike_id = 'bike_5')
     *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  SELECT data.minute, data.bike_id, b.owner_name FROM bikes b INNER JOIN LATERAL (
     * 	SELECT time_bucket(interval '1 min', time) AS minute, bike_id FROM test t
     * 	WHERE t.bike_id = b.bike_id AND time > '2018-08-29 18:00:00.0'
     * 	AND s_17 IS NOT NULL
     * 	GROUP BY minute, bike_id
     * 	HAVING AVG(s_17) > 3.000000
     * 	ORDER BY minute DESC LIMIT 1
     *  ) AS data
     *  ON true
     *  ORDER BY b.bike_id, data.minute DESC;
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status lastTimeActivelyDriven(Query query) {
        Sensor sensor = query.getSensor();
        List<String> columns = new ArrayList<>(
                Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName())
        );
        String sql = "";

        Timestamp timestamp = new Timestamp(query.getStartTimestamp());
        sql = "WITH last_trip AS (\n" +
                "\tSELECT date_trunc('minute', time) AS minute, bike_id FROM %s t \n" +
                "\tWHERE time > '%s'\n" +
                "\tGROUP BY minute, bike_id\n" +
                "\tHAVING AVG(%s) > %f\n" +
                "\tORDER BY minute\n" +
                ")\n" +
                "SELECT DISTINCT ON (l.bike_id) l.minute, l.bike_id \n" +
                "FROM last_trip l ORDER BY l.bike_id, l.minute DESC;";
        sql = String.format(
                Locale.US,
                sql,
                tableName,
                timestamp,
                sensor.getName(),
                query.getThreshold()
        );
        return executeQuery(sql);
    }

    /**
     * Groups entries within a time range in time buckets and by bikes and aggregates values in each time bucket.
     *
     * NARROW_TABLE:
     * <p><code>
     *  SELECT time_bucket(interval '300000 ms', time) as time_bucket, AVG(value), bike_id FROM emg_benchmark
     *  WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0') GROUP BY time_bucket, bike_id;
     * </code></p>
     *
     * WIDE_TABLE:
     * <p><code>
     *  with downsample as (
     * 	select date_trunc('minutes', time) as minute, bike_id, avg(s_40) as value
     * 	from test t
     * 	where bike_id = 'bike_51'
     * 	and time >= '2018-08-30 02:00:00.0'
     * 	and time <= '2018-08-30 03:00:00.0'
     * 	group by bike_id, minute
     * ) select d.minute, b.bike_id, b.owner_name, d.value
     * from downsample d, bikes b where b.bike_id = d.bike_id;
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status downsample(Query query) {
        Sensor sensor = query.getSensor();
        Bike bike = query.getBikes().get(0);
        Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
        String sql = "";
        sql = "with downsample as (\n" +
                "\tselect date_trunc('minutes', time) as minute, bike_id, avg(%s) as value\n" +
                "\tfrom %s t \n" +
                "\twhere bike_id = '%s'\n" +
                "\tand time >= '%s'\n" +
                "\tand time <= '%s'\n" +
                "\tgroup by bike_id, minute\n" +
                ") select d.minute, b.bike_id, b.owner_name, d.value \n" +
                "from downsample d, bikes b where b.bike_id = d.bike_id;";
        sql = String.format(
                Locale.US,
                sql,
                sensor.getName(),
                tableName,
                bike.getName(),
                startTimestamp,
                endTimestamp
        );

        return executeQuery(sql);
    }

    /**
     * Creates a heat map with average air quality out of gps points.
     *
     * <p><code>
     *  select st_x(s_12::geometry) as longitude, st_y(s_12::geometry) as latitude, avg(s_34)
     *  from test t
     *  where s_12 is not null and time >= '2018-08-30 02:00:00.0' and time <= '2018-08-30 03:00:00.0'
     *  group by s_12;
     * </code></p>
     * @param query The heatmap query paramters object.
     * @return The status of the execution.
     */
    @Override
    public Status airPollutionHeatMap(Query query) {
        Sensor sensor = query.getSensor();
        Sensor gpsSensor = query.getGpsSensor();
        Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
        GeoPoint startPoint = Constants.GRID_START_POINT;
        String sql = "";

        sql = "select st_x(%s::geometry) as longitude, st_y(%s::geometry) as latitude, avg(%s) from %s t \n" +
                "where %s is not null and time >= '%s' and time <= '%s' group by %s having avg(%s) > %f;";
        sql = String.format(Locale.US, sql,
                gpsSensor.getName(),
                gpsSensor.getName(),
                sensor.getName(),
                tableName,
                gpsSensor.getName(),
                startTimestamp,
                endTimestamp,
                gpsSensor.getName(),
                sensor.getName(),
                query.getThreshold()
        );
        return executeQuery(sql);
    }



    /**
     * Selects bikes whose last gps location lies in a certain area.
     *
     * NARROW_TABLE:
     * <p><code>
     *  with last_location as (
     * 	select bike_id, last(value, time) as location from gps_benchmark group by bike_id
     *  ) select * from last_location l
     *  where st_contains(
     *    st_buffer(st_setsrid(st_makepoint(13.431947, 48.566736),4326)::geography, 500)::geometry,
     *    l.location::geometry
     *  );
     * </code></p>
     *
     * WIDE_TABLE:
     *  <p><code>
     *  with data as (
     * 	 select distinct on (bike_id) bike_id, s_12 as location from test t
     * 	 where s_12 is not null
     * 	 group by bike_id, time, s_12
     * 	 order by bike_id, time desc
     *  ) select b.bike_id, b.owner_name, d.location from data d, bikes b
     *  where b.bike_id = d.bike_id
     *  and st_contains(st_buffer(
     * 		st_setsrid(st_makepoint(13.431947, 48.566736),4326)::geography, 500
     * 	)::geometry, d.location::geometry);
     * </code></p>
     * @param query
     * @return
     */
    @Override
    public Status bikesInLocation(Query query) {
        String sql = "";
        Sensor gpsSensor = query.getGpsSensor();
        sql = "with data as (\n" +
                "\tselect distinct on (bike_id) bike_id, %s as location from %s t\n" +
                "\twhere %s is not null\n" +
                "\tgroup by bike_id, time, %s\n" +
                "\torder by bike_id, time desc\n" +
                ") select b.bike_id, b.owner_name, d.location from data d, bikes b \n" +
                "where b.bike_id = d.bike_id\n" +
                "and st_contains(st_buffer(\n" +
                "\t\tst_setsrid(st_makepoint(%f, %f),4326)::geography, %d\n" +
                "\t)::geometry, d.location::geometry);";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
                gpsSensor.getName(),
                gpsSensor.getName(),
                Constants.SPAWN_POINT.getLongitude(),
                Constants.SPAWN_POINT.getLatitude(),
                config.RADIUS
        );
        return executeQuery(sql);
    }

    /*
     * Executes the SQL query and measures the execution time.
     */
    private Status executeQuery(String sql) {
        LOGGER.info("{} executes the SQL query: {}", Thread.currentThread().getName(), sql);
        long st;
        long en;
        int line = 0;
        int queryResultPointNum = 0;
        try (Statement statement = connection.createStatement()){
            st = System.nanoTime();
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    line++;
                }
            }
            en = System.nanoTime();
            queryResultPointNum = line;
            return new Status(true, en - st, queryResultPointNum);
        } catch (Exception e) {
            return new Status(false, 0, queryResultPointNum, e, sql);
        }
    }

    /*
     * Returns an SQL statement, which creates a wide table for all sensors at once.
     */
    private String getCreateTableSql() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE ").append(tableName).append(" (time TIMESTAMPTZ NOT NULL, ")
                .append("bike_id VARCHAR(20) REFERENCES bikes (bike_id)");
        config.SENSORS.forEach(sensor -> sqlBuilder.append(", ").append(sensor.getName()).append(" ")
                .append(sensor.getDataType()));
        sqlBuilder.append(") PARTITION BY RANGE (time);");
        return sqlBuilder.toString();
    }

    /*
     * Returns an SQL batch insert query for insert bikes meta data.
     */
    private String getInsertBikesSql(List<Bike> bikesList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO bikes (bike_id, owner_name) VALUES ");

        boolean firstIteration = true;
        for (int i = 0; i < bikesList.size(); i++) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                sqlBuilder.append(", ");
            }

            Bike bike = bikesList.get(i);
            sqlBuilder.append("('").append(bike.getName()).append("', ")
                    .append("'").append(nameFaker.name().firstName()).append("')");
        }
        sqlBuilder.append(";");
        return sqlBuilder.toString();
    }

    /*
     * Returns an SQL statement, which inserts a batch of points into single wide table.
     */
    private String getInsertOneWideBatchSql(Batch batch) {
        Map<Long, List<String>> rows = transformBatch(batch);
        StringBuilder sqlBuilder = new StringBuilder();
        Bike bike = batch.getBike();
        sqlBuilder.append("INSERT INTO ").append(tableName).append(" VALUES ");
        boolean firstIteration = true;
        for (long t : rows.keySet()) {
            if (firstIteration) {
                firstIteration = false;
            } else {
                sqlBuilder.append(", ");
            }
            Timestamp timestamp = new Timestamp(t);
            sqlBuilder.append("('").append(timestamp).append("', '").append(bike.getName()).append("'");
            List<String> valuesList = rows.get(t);
            valuesList.forEach(value -> sqlBuilder.append(", ").append(value));
            sqlBuilder.append(")");
        }
        return sqlBuilder.toString();
    }

    /*
     * Transforms a batch from column-oriented format (each column represents readings of one sensor group) to
     * row-oriented format.
     */
    private Map<Long, List<String>> transformBatch(Batch batch) {
        Map<Sensor, Point[]> entries = batch.getEntries();
        Map<Long, List<String>> rows = new TreeMap<>();

        List<String> emptyRow = new ArrayList<>(config.SENSORS.size());
        for (Sensor sensor : config.SENSORS) {
            emptyRow.add("NULL");
        }

        Sensor mostFrequentSensor = Sensors.minInterval(config.SENSORS);
        for (Point point : entries.get(mostFrequentSensor)) {
            rows.computeIfAbsent(point.getTimestamp(), k -> new ArrayList<>(emptyRow));
        }

        for (int i = 0; i < config.SENSORS.size(); i++) {
            for (Point point : entries.get(config.SENSORS.get(i))) {
                rows.get(point.getTimestamp()).set(i, point.getValue());
            }
        }
        return rows;
    }
}
