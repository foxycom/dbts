package cn.edu.tsinghua.iotdb.benchmark.tsdb.cratedb;

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
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class CrateDB implements IDatabase {

    private static final int B2GB = 1024 * 1024 * 1024;
    private Connection connection;
    private static String tableName;
    private static Config config;
    private static final Logger LOGGER = LoggerFactory.getLogger(cn.edu.tsinghua.iotdb.benchmark.tsdb.memsql.MemSQL.class);
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s;";
    private SqlBuilder sqlBuilder;
    private Faker nameFaker;

    /**
     * Initializes an instance of the database controller.
     */
    public CrateDB() {
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
                    String.format(Constants.CRATE_URL, config.HOST, config.PORT),
                    Constants.CRATE_USER,
                    Constants.CRATE_PASSWD
            );
        } catch (Exception e) {
            LOGGER.error("Initialize CrateDB failed because ", e);
            throw new TsdbException(e);
        }
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
            statement.addBatch(String.format(DROP_TABLE, tableName));
            statement.executeBatch();
            connection.commit();

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

            // Creates bikes' relational data table.
            statement.execute("CREATE TABLE IF NOT EXISTS bikes (bike_id TEXT, owner_name TEXT NOT NULL, " +
                    "PRIMARY KEY (bike_id));");

            // Inserts all bikes.
            statement.execute(getInsertBikesSql(schemaList));

            statement.execute(getCreateTableSql());
        } catch (SQLException e) {
            LOGGER.error("Can't create CrateDB table because: {}", e.getMessage());
            System.out.println(e.getNextException().getMessage());
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
        return 0;
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
     * <p><code>
     *  SELECT time, bike_id, s_33 FROM test WHERE (bike_id = 'bike_0') AND (time = '2018-08-30 02:00:00.0');
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
        String debug = sqlBuilder.build();
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
     *  SELECT LAST_VALUE(t.s_12) OVER(PARTITION BY t.bike_id ORDER BY (time)),
     *  MAX(time), t.bike_id, b.owner_name FROM test t, bikes b
     *  WHERE t.s_12 IS NOT NULL AND t.bike_id = b.bike_id
     *  GROUP BY t.bike_id;
     * </code></p>
     *
     * @param query The query parameters query.
     * @return The status of the execution.
     */
    @Override
    public Status lastKnownPosition(Query query) {
        Sensor gpsSensor = query.getGpsSensor();
        String sql = "";

        sql = "SELECT LAST_VALUE(t.%s) OVER(PARTITION BY t.bike_id ORDER BY (time)), \n" +
                "MAX(time), t.bike_id, b.owner_name FROM %s t, bikes b\n" +
                "WHERE t.%s IS NOT NULL AND t.bike_id = b.bike_id\n" +
                "GROUP BY t.bike_id;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
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
     *  SELECT s_12 AS location, t.bike_id, b.owner_name FROM test t, bikes b
     * 	WHERE b.bike_id = t.bike_id
     * 	AND t.bike_id = 'bike_5'
     * 	AND s_12 IS NOT NULL
     * 	AND time >= '2018-08-30 02:00:00.0'
     * 	AND time <= '2018-08-30 03:00:00.0';
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
        String sql = "";

        sql = "SELECT %s AS location, t.bike_id, b.owner_name FROM %s t, bikes b\n" +
                "\tWHERE b.bike_id = t.bike_id\n" +
                "\tAND t.bike_id = '%s' \n" +
                "\tAND %s IS NOT NULL\n" +
                "\tAND time >= '%s' \n" +
                "\tAND time <= '%s';\n";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
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
     *  WITH data AS (
     * 	 SELECT from_unixtime(unix_timestamp(time) DIV 1 * 1) AS SECOND,
     * 	 bike_id, s_12
     * 	 FROM test t
     * 	 WHERE bike_id = 'bike_8'
     * 	 AND s_12 IS NOT NULL
     * 	 AND time >= '2018-08-30 02:00:00.0'
     * 	 AND time < '2018-08-30 03:00:00.0'
     * 	 GROUP BY second, bike_id, s_12
     * 	 HAVING AVG(s_17) >= 1000.000000
     * )
     * SELECT d.second, d.bike_id, b.owner_name, d.s_12 FROM bikes b, data d
     * WHERE d.bike_id = b.bike_id
     * ORDER BY d.second ASC, d.bike_id;
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

        sql = "WITH data AS (\n" +
                "\tSELECT from_unixtime(unix_timestamp(time) DIV 1 * 1) AS SECOND, \n" +
                "\tbike_id, %s\n" +
                "\tFROM %s t \n" +
                "\tWHERE bike_id = '%s'\n" +
                "\tAND %s IS NOT NULL \n" +
                "\tAND time >= '%s' \n" +
                "\tAND time < '%s'\n" +
                "\tGROUP BY second, bike_id, %s\n" +
                "\tHAVING AVG(%s) >= %f\n" +
                ")\n" +
                "SELECT d.second, d.bike_id, b.owner_name, d.%s FROM bikes b, data d\n" +
                "WHERE d.bike_id = b.bike_id\n" +
                "ORDER BY d.second ASC, d.bike_id;";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName,
                bike.getName(),
                gpsSensor.getName(),
                startTimestamp,
                endTimestamp,
                gpsSensor.getName(),
                sensor.getName(),
                query.getThreshold(),
                gpsSensor.getName()
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
     *  with data as (
     * 	 select from_unixtime(unix_timestamp(time) DIV 1 * 1) as second, bike_id, s_12
     * 	 from test t
     * 	 where bike_id = 'bike_5'
     * 	 and time >= '2018-08-30 02:00:00.0' and time <= '2018-08-30 03:00:00.0'
     * 	 group by second, bike_id, s_12
     * 	 having avg(s_17) > 1000.0 and s_12 is not null
     * 	 order by second
     *  )
     *  select d.bike_id, b.owner_name,
     *  geography_length(
     *     concat('LINESTRING(', group_concat(concat(geography_longitude(s_12), ' ', geography_latitude(s_12))), ')')
     *  ) from data d, bikes b
     *  where d.bike_id = b.bike_id
     *  group by d.bike_id;
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
                "\tselect from_unixtime(unix_timestamp(time) DIV 1 * 1) as second, bike_id, %s \n" +
                "\tfrom %s t\n" +
                "\twhere bike_id = '%s' \n" +
                "\tand time >= '%s' and time <= '%s'\n" +
                "\tgroup by second, bike_id, %s\n" +
                "\thaving avg(%s) > %s and %s is not null\n" +
                "\torder by second\n" +
                ")\n" +
                "select d.bike_id, b.owner_name, \n" +
                "geography_length(\n" +
                "    concat('LINESTRING(', group_concat(concat(geography_longitude(%s), ' ', geography_latitude(%s))), ')')\n" +
                ") from data d, bikes b\n" +
                "where d.bike_id = b.bike_id\n" +
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
                gpsSensor.getName(),
                gpsSensor.getName()
        );
        return executeQuery(sql);
    }

    /**
     * Selects bikes that did not send any data since a given timestamp.
     *
     * <p><code>
     *  SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
     * 	 SELECT DISTINCT(bike_id) FROM test t WHERE time > '2018-08-30 03:00:00.0'
     *  );
     * </code></p>
     * @param query The query params object.
     * @return Status of the execution.
     */
    @Override
    public Status offlineBikes(Query query) {
        String sql = "";
        Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());

        sql = "SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (\n" +
                "\tSELECT DISTINCT(bike_id) FROM %s t WHERE time > '%s'\n" +
                ");";
        sql = String.format(
                Locale.US,
                sql,
                tableName,
                endTimestamp
        );
        return executeQuery(sql);
    }

    /**
     *
     * TODO probably include also gps coordinates or path segment
     * Scans metrics of a sensor in a specific time interval and selects those which have a certain value.

     * <p><code>
     *  SELECT l.bike_id, MAX(l.minute) as last_time FROM test t, (
     *   SELECT date_trunc('minute', "time") as minute, bike_id
     *   FROM test t WHERE time > '2018-08-30 03:00:00.0'
     *   GROUP BY minute, bike_id
     *   HAVING AVG(s_17) > 1000.000000
     *  ) as l
     *  WHERE l.bike_id = t.bike_id
     *  GROUP BY l.bike_id limit 100;
     * </code></p>
     *
     * @param query The query parameters object.
     * @return The status of the execution.
     */
    @Override
    public Status lastTimeActivelyDriven(Query query) {
        Sensor sensor = query.getSensor();
        String sql = "";

        Timestamp timestamp = new Timestamp(query.getEndTimestamp());
        sql = "SELECT l.bike_id, MAX(l.minute) as last_time FROM %s t, (\n" +
                "SELECT date_trunc('minute', \"time\") as minute, bike_id\n" +
                "FROM %s t WHERE time > '%s'\n" +
                "GROUP BY minute, bike_id\n" +
                "HAVING AVG(%s) > %f  \n" +
                ") as l\n" +
                "WHERE l.bike_id = t.bike_id\n" +
                "GROUP BY l.bike_id limit 100;";
        sql = String.format(
                Locale.US,
                sql,
                tableName,
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
     *  WITH downsample AS (
     * 	 SELECT from_unixtime(unix_timestamp(time) DIV 60 * 60) AS minute, bike_id, AVG(s_27) AS value
     * 	 FROM test t
     * 	 WHERE bike_id = 'bike_8'
     * 	 AND time >= '2018-08-30 02:00:00.0'
     * 	 AND time <= '2018-08-30 03:00:00.0'
     * 	 GROUP BY bike_id, minute
     *  ) SELECT d.minute, b.bike_id, b.owner_name, d.value
     *  FROM downsample d, bikes b WHERE b.bike_id = d.bike_id
     *  ORDER BY d.minute, b.bike_id;
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
        sql = "WITH downsample AS (\n" +
                "\tSELECT from_unixtime(unix_timestamp(time) DIV 60 * 60) AS minute, bike_id, AVG(%s) AS value\n" +
                "\tFROM %s t \n" +
                "\tWHERE bike_id = '%s'\n" +
                "\tAND time >= '%s'\n" +
                "\tAND time <= '%s'\n" +
                "\tGROUP BY bike_id, minute\n" +
                ") SELECT d.minute, b.bike_id, b.owner_name, d.value \n" +
                "FROM downsample d, bikes b WHERE b.bike_id = d.bike_id\n" +
                "ORDER BY d.minute, b.bike_id;";
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
     *  SELECT GEOGRAPHY_LONGITUDE(s_12) as longitude, GEOGRAPHY_LATITUDE(s_12) as latitude, AVG(s_34)
     *  FROM test t
     *  WHERE s_12 IS NOT NULL AND time >= '2018-08-30 02:00:00.0' AND time <= '2018-08-30 03:00:00.0'
     *  GROUP BY s_12;
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
        String sql = "";

        sql = "SELECT longitude(%s) as longitude, latitude(%s) as latitude, AVG(%s) \n" +
                "FROM %s t \n" +
                "WHERE time >= '%s' AND time <= '%s' \n" +
                "GROUP BY longitude, latitude order by longitude, latitude;";
        sql = String.format(Locale.US, sql,
                gpsSensor.getName(),
                gpsSensor.getName(),
                sensor.getName(),
                tableName,
                startTimestamp,
                endTimestamp
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
     *  select b.bike_id, b.owner_name, pos.pos from bikes b,
     * 	(select bike_id, last_value(s_12) over (partition by bike_id order by (time)) as pos from test
     * 	group by bike_id) as pos
     *  where b.bike_id = pos.bike_id
     *  and geography_contains('POLYGON((13.4406567 48.5723195,
     *  13.4373522 48.5707861, 13.4373522 48.5662708,
     *  13.4443045 48.5645384, 13.4489393 48.5683155,
     *  13.4492826 48.5710701, 13.4406567 48.5723195))', pos.pos);
     * </code></p>
     * @param query
     * @return
     */
    @Override
    public Status bikesInLocation(Query query) {
        String sql = "";
        Sensor gpsSensor = query.getGpsSensor();
        sql = "select b.bike_id, b.owner_name, pos.pos from bikes b, \n" +
                "\t(select bike_id, last_value(%s) over (partition by bike_id order by (time)) as pos from %s \n" +
                "\tgroup by bike_id) as pos \n" +
                "where b.bike_id = pos.bike_id \n" +
                "and geography_contains('POLYGON((13.4406567 48.5723195, \n" +
                "13.4373522 48.5707861, 13.4373522 48.5662708, \n" +
                "13.4443045 48.5645384, 13.4489393 48.5683155, \n" +
                "13.4492826 48.5710701, 13.4406567 48.5723195))', pos.pos);";
        sql = String.format(
                Locale.US,
                sql,
                gpsSensor.getName(),
                tableName
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
        sqlBuilder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (time TIMESTAMPTZ NOT NULL, ")
                .append("bike_id TEXT");
        config.SENSORS.forEach(sensor -> sqlBuilder.append(", ").append(sensor.getName()).append(" ")
                .append(sensor.getDataType()));
        sqlBuilder.append(")");
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
        String debug = sqlBuilder.toString();
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
