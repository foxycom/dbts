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
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;
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
  private SqlBuilder sqlBuilder;

  /** Initializes an instance of the database controller. */
  public Citus() {
    sqlBuilder = new SqlBuilder();
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
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
      connection =
          DriverManager.getConnection(
              String.format(Constants.POSTGRESQL_URL, config.HOST, config.PORT, config.DB_NAME),
              Constants.POSTGRESQL_USER,
              Constants.POSTGRESQL_PASSWD);
    } catch (Exception e) {
      LOGGER.error("Initialize Citus failed because ", e);
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
    // delete old data
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.addBatch(String.format(DROP_TABLE, "bikes"));
      statement.addBatch(String.format(DROP_TABLE, "grid"));
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
   *
   * <ul>
   *   <li>Sensor group name -> table name
   *   <li>Bike name -> a field in table
   *   <li>Sensor name -> a field in table
   *   <li>Value(s) -> fields in table
   * </ul>
   *
   * <p>Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables -- We
   * start by creating a regular PG table:
   *
   * <p><code>
   * CREATE TABLE gps_benchmark (time TIMESTAMPTZ NOT NULL, bike_id VARCHAR(20) NOT NULL,
   *    sensor_id VARCHAR(20) NOT NULL, value REAL NULL);
   * </code> -- This creates a hypertable that is partitioned by time using the values in the `time`
   * column.
   *
   * <p><code>
   * SELECT create_hypertable('gps_benchmark', 'time', chunk_time_interval => interval '1 h');
   * </code>
   */
  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      createGridFunction(statement);

      // Creates bikes' relational data table.
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS bikes (bike_id VARCHAR PRIMARY KEY, owner_name VARCHAR(100) NOT NULL);");
      statement.addBatch("CREATE INDEX ON bikes (bike_id);");

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
      String selectSizeSql =
          "SELECT logicalrelid AS name, "
              + "citus_total_relation_size(logicalrelid) AS size "
              + "FROM pg_dist_partition";
      ResultSet rs = statement.executeQuery(selectSizeSql);
      long resultInB = 0L;
      while (rs.next()) {
        resultInB += rs.getLong("size");
      }
      resultInGB = (float) resultInB / B2GB;
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
      String createGeoIndexSql =
          "CREATE INDEX ON " + tableName + " USING GIST (" + sensor.getName() + ")";
      statement.addBatch(createGeoIndexSql);
    }

    statement.executeBatch();
    connection.commit();
  }

  private void distributeTables(Statement statement) throws SQLException {
    connection.setAutoCommit(false);
    statement.execute("SELECT create_distributed_table('bikes', 'bike_id');");
    statement.execute("SELECT create_distributed_table('test', 'bike_id');");
    statement.execute(
        "CREATE TABLE test_2018_08_00 PARTITION OF test FOR VALUES FROM ('2018-08-30') TO ('2018-08-31');");
    connection.commit();
  }

  /*
   * Registers a function in SQL, which generates a grid for a heat map.
   */
  private void createGridFunction(Statement statement) throws SQLException {
    String sql =
        "CREATE OR REPLACE FUNCTION ST_CreateGrid(\n"
            + "nrow integer, ncol integer,\n"
            + "xsize float8, ysize float8,\n"
            + "x0 float8 DEFAULT 0, y0 float8 DEFAULT 0,\n"
            + "OUT \"row\" integer, OUT col integer,\n"
            + "OUT geom geometry)\n"
            + "RETURNS SETOF record AS\n"
            + "$$\n"
            + "SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom\n"
            + "FROM generate_series(0, $1 - 1) AS i,\n"
            + "generate_series(0, $2 - 1) AS j,\n"
            + "(\n"
            + "SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell\n"
            + ") AS foo;\n"
            + "$$ LANGUAGE sql IMMUTABLE STRICT;";
    statement.executeUpdate(sql);
  }

  private String getCreateGridTableSql() {
    String sql =
        "create table grid as \n"
            + "select (st_dump(map.geom)).geom from (\n"
            + "\tselect st_setsrid(st_collect(grid.geom),4326) as geom \n"
            + "\tfrom ST_CreateGrid(40, 90, 0.0006670, 0.0006670, %f, %f) as grid\n"
            + ") as map;";
    sql =
        String.format(
            Locale.US,
            sql,
            Constants.GRID_START_POINT.getLongitude(),
            Constants.GRID_START_POINT.getLatitude());
    return sql;
  }

  /**
   * Writes the given batch of data points into the database and measures the time the database
   * takes to store the points.
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
   *  SELECT time, b.bike_id, b.owner_name, s_33
   *  FROM test t, bikes b
   *  WHERE b.bike_id = t.bike_id
   *  AND b.bike_id = 'bike_51'
   *  AND time = '2018-08-30 02:00:00.0';
   * </code>
   *
   * @param query The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status precisePoint(Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    Timestamp timestamp = new Timestamp(query.getStartTimestamp());
    String sql =
        "SELECT time, b.bike_id, b.owner_name, %s \n"
            + "FROM %s t, bikes b\n"
            + "WHERE b.bike_id = t.bike_id\n"
            + "AND b.bike_id = '%s' \n"
            + "AND time = '%s';";
    sql = String.format(Locale.US, sql, sensor.getName(), tableName, bike.getName(), timestamp);
    return executeQuery(sql);
  }

  /**
   * Selects the last known GPS position of bikes.
   *
   * <p><code>
   *  WITH location AS (
   * 	 SELECT DISTINCT ON(bike_id) bike_id, s_12, time
   * 	 FROM test t
   * 	 WHERE s_12 IS NOT NULL
   * 	 GROUP BY bike_id, time, s_12
   * 	 ORDER BY bike_id, time desc
   *  ) SELECT l.time, b.bike_id, b.owner_name, l.s_12
   *  FROM location l, bikes b WHERE b.bike_id = l.bike_id;
   * </code>
   *
   * @param query The query parameters query.
   * @return The status of the execution.
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    String sql = "";

    sql =
        "WITH location AS (\n"
            + "\tSELECT DISTINCT ON(bike_id) bike_id, %s, time \n"
            + "\tFROM %s t\n"
            + "\tGROUP BY bike_id, time, %s\n"
            + "\tORDER BY bike_id, time desc\n"
            + ") SELECT l.time, b.bike_id, b.owner_name, l.%s \n"
            + "FROM location l, bikes b WHERE b.bike_id = l.bike_id;";
    sql =
        String.format(
            Locale.US,
            sql,
            gpsSensor.getName(),
            tableName,
            gpsSensor.getName(),
            gpsSensor.getName());

    return executeQuery(sql);
  }

  /**
   * Performs a scan of gps data points in a given time range for specified number of bikes.
   *
   * <p><code>
   *  SELECT data.bike_id, b.owner_name, data.location FROM bikes b INNER JOIN LATERAL (
   * 	 SELECT s_12 AS location, bike_id FROM test t
   * 	 WHERE t.bike_id = b.bike_id
   * 	 AND bike_id = 'bike_67'
   * 	 AND time >= '2018-08-30 02:00:00.0'
   * 	 AND time <= '2018-08-30 03:00:00.0'
   *  ) AS data ON true;
   * </code>
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
    List<String> columns =
        new ArrayList<>(
            Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
    String sql = "";

    sql =
        "SELECT data.bike_id, b.owner_name, data.location FROM bikes b INNER JOIN LATERAL (\n"
            + "\tSELECT %s AS location, bike_id FROM test t\n"
            + "\tWHERE t.bike_id = b.bike_id \n"
            + "\tAND bike_id = '%s' \n"
            + "\tAND time >= '%s' \n"
            + "\tAND time <= '%s'\n"
            + ") AS data ON true;";
    sql =
        String.format(
            Locale.US, sql, gpsSensor.getName(), bike.getName(), startTimestamp, endTimestamp);
    return executeQuery(sql);
  }

  /**
   * Identifies active trips, when current value is above a threshold.
   *
   * <p><code>
   *  SELECT data.second, data.bike_id, b.owner_name, data.s_12 FROM bikes b INNER JOIN LATERAL (
   * 	 SELECT date_trunc('second', time) AS second, bike_id, s_12
   * 	 FROM test t
   * 	 WHERE t.bike_id = b.bike_id
   * 	 AND t.bike_id = 'bike_51'
   * 	 AND s_12 IS NOT NULL
   * 	 AND time >= '2018-08-30 02:00:00.0'
   * 	 AND time < '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, s_12
   * 	 HAVING AVG(s_17) >= 1000.000000
   *  ) AS data ON true
   *  ORDER BY data.second asc, data.bike_id;
   * </code>
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

    sql =
        "SELECT data.second, data.bike_id, b.owner_name, data.%s FROM bikes b INNER JOIN LATERAL (\n"
            + "\tSELECT date_trunc('second', time) AS second, bike_id, %s\n"
            + "\tFROM test t \n"
            + "\tWHERE t.bike_id = b.bike_id \n"
            + "\tAND t.bike_id = '%s'\n"
            + "\tAND time >= '%s' \n"
            + "\tAND time < '%s'\n"
            + "\tGROUP BY second, bike_id, %s\n"
            + "\tHAVING AVG(%s) >= %f\n"
            + ") AS data ON true\n"
            + "ORDER BY data.second asc, data.bike_id;";
    sql =
        String.format(
            Locale.US,
            sql,
            gpsSensor.getName(),
            gpsSensor.getName(),
            bike.getName(),
            startTimestamp,
            endTimestamp,
            gpsSensor.getName(),
            sensor.getName(),
            query.getThreshold());

    return executeQuery(sql);
  }

  /**
   * Computes the distance driven by a bike in the given time range identified by start and end
   * timestamps where current exceeds some value.
   *
   * <p><code>
   *  WITH data AS (
   * 	 SELECT date_trunc('second', time) AS second, bike_id, s_12
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_51'
   * 	 AND time >= '2018-08-30 02:00:00.0' AND time <= '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, s_12
   * 	 HAVING AVG(s_17) > 1000.000000 AND s_12 IS NOT NULL
   * 	 ORDER BY second
   *  )
   *  SELECT d.bike_id, ST_LENGTH(ST_MAKELINE(d.s_12::geometry)::geography)
   *  FROM data d
   *  GROUP BY d.bike_id;
   * </code>
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

    sql =
        "WITH data AS (\n"
            + "\tSELECT date_trunc('second', time) AS second, bike_id, %s \n"
            + "\tFROM %s t\n"
            + "\tWHERE bike_id = '%s' \n"
            + "\tAND time >= '%s' AND time <= '%s'\n"
            + "\tGROUP BY second, bike_id, %s\n"
            + "\tHAVING AVG(%s) > %f\n"
            + "\tORDER BY second\n"
            + ")\n"
            + "SELECT d.bike_id, ST_LENGTH(ST_MAKELINE(d.%s::geometry)::geography) \n"
            + "FROM data d\n"
            + "GROUP BY d.bike_id;";
    sql =
        String.format(
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
            gpsSensor.getName());
    return executeQuery(sql);
  }

  /**
   * Computes road segments with high average sensor measurements values, e.g., accelerometer,
   * identifying spots where riders often use brakes.
   *
   * <p>NARROW_TABLE:
   *
   * <p><code>
   *  WITH trip AS (
   * 	SELECT time_bucket(interval '1 s', time) AS second, bike_id, AVG(value) AS value FROM emg_benchmark
   * 	WHERE value > 3.000000 AND bike_id = 'bike_10'
   * 	GROUP BY second, bike_id
   *  )
   *  SELECT time_bucket(interval '300000 ms', g.time) AS half_minute, AVG(t.value), t.bike_id, st_makeline(g.value::geometry) FROM gps_benchmark g, trip t
   *  WHERE g.bike_id = 'bike_10' AND g.time = t.second AND g.time > '2018-08-29 18:00:00.0' AND g.time < '2018-08-29 19:00:00.0'
   *  GROUP BY half_minute, t.bike_id;
   * </code> WIDE_TABLE:
   *
   * <p><code>
   *  SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   * 	 SELECT DISTINCT(bike_id) FROM test t WHERE time > '2018-08-30 03:00:00.0'
   *  );
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status offlineBikes(Query query) {
    String sql = "";
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());

    sql =
        "SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (\n"
            + "\tSELECT DISTINCT(bike_id) FROM %s t WHERE time > '%s'\n"
            + ");";
    sql = String.format(Locale.US, sql, tableName, endTimestamp);
    return executeQuery(sql);
  }

  /**
   * Scans metrics of a sensor in a specific time interval and selects those which have a certain
   * value.
   *
   * <p><code>
   *  WITH last_trip AS (
   * 	 SELECT date_trunc('minute', time) AS minute, bike_id FROM test t
   * 	 WHERE time > '2018-08-30 02:00:00.0'
   * 	 GROUP BY minute, bike_id
   * 	 HAVING AVG(s_17) > 1000.000000
   * 	 ORDER BY minute
   *  )
   *  SELECT DISTINCT ON (b.bike_id) l.minute, b.bike_id, b.owner_name
   *  FROM last_trip l, bikes b WHERE b.bike_id = l.bike_id
   *  ORDER BY b.bike_id, l.minute DESC;
   * </code>
   *
   * @param query The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();
    String sql = "";

    Timestamp timestamp = new Timestamp(query.getEndTimestamp());
    sql =
        "WITH last_trip AS (\n"
            + "\tSELECT date_trunc('minute', time) AS minute, bike_id FROM %s t \n"
            + "\tWHERE time > '%s'\n"
            + "\tGROUP BY minute, bike_id\n"
            + "\tHAVING AVG(%s) > %f\n"
            + "\tORDER BY minute\n"
            + ")\n"
            + "SELECT DISTINCT ON (b.bike_id) l.minute, b.bike_id, b.owner_name\n"
            + "FROM last_trip l, bikes b WHERE b.bike_id = l.bike_id \n"
            + "ORDER BY b.bike_id, l.minute DESC;";
    sql =
        String.format(Locale.US, sql, tableName, timestamp, sensor.getName(), query.getThreshold());
    return executeQuery(sql);
  }

  /**
   * Groups entries within a time range in time buckets and by bikes and aggregates values in each
   * time bucket.
   *
   * <p><code>
   *  WITH downsample AS (
   * 	 SELECT date_trunc('minutes', time) as minute, bike_id, AVG(s_27) AS value
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_51'
   * 	 AND time >= '2018-08-30 02:00:00.0'
   * 	 AND time <= '2018-08-30 03:00:00.0'
   * 	 GROUP BY bike_id, minute
   *  ) SELECT d.minute, b.bike_id, b.owner_name, d.value
   *  FROM downsample d, bikes b WHERE b.bike_id = d.bike_id
   *  ORDER BY d.minute, b.bike_id;
   * </code>
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
    sql =
        "WITH downsample AS (\n"
            + "\tSELECT date_trunc('minutes', time) as minute, bike_id, AVG(%s) AS value\n"
            + "\tFROM %s t \n"
            + "\tWHERE bike_id = '%s'\n"
            + "\tAND time >= '%s'\n"
            + "\tAND time <= '%s'\n"
            + "\tGROUP BY bike_id, minute\n"
            + ") SELECT d.minute, b.bike_id, b.owner_name, d.value \n"
            + "FROM downsample d, bikes b WHERE b.bike_id = d.bike_id \n"
            + "ORDER BY d.minute, b.bike_id;";
    sql =
        String.format(
            Locale.US,
            sql,
            sensor.getName(),
            tableName,
            bike.getName(),
            startTimestamp,
            endTimestamp);

    return executeQuery(sql);
  }

  /**
   * Creates a heat map with average air quality out of gps points.
   *
   * <p><code>
   *  SELECT ST_X(s_12::geometry) AS longitude, ST_Y(s_12::geometry) AS latitude, AVG(s_34) FROM test t
   *  WHERE time >= '2018-08-30 02:00:00.0' AND time <= '2018-08-30 03:00:00.0'
   *  GROUP BY longitude, latitude;
   * </code>
   *
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

    sql =
        "SELECT ST_X(%s::geometry) AS longitude, ST_Y(%s::geometry) AS latitude, AVG(%s) FROM %s t \n"
            + "WHERE time >= '%s' AND time <= '%s' GROUP BY longitude, latitude;";
    sql =
        String.format(
            Locale.US,
            sql,
            gpsSensor.getName(),
            gpsSensor.getName(),
            sensor.getName(),
            tableName,
            startTimestamp,
            endTimestamp);
    return executeQuery(sql);
  }

  /**
   * Selects bikes whose last gps location lies in a certain area.
   *
   * <p><code>
   *  WITH data AS (
   * 	 SELECT DISTINCT ON (bike_id) bike_id, s_12 AS location FROM test t
   * 	 GROUP BY bike_id, time, s_12
   * 	 ORDER BY bike_id, time DESC
   *  ) SELECT b.bike_id, b.owner_name, d.location from data d, bikes b
   *  WHERE b.bike_id = d.bike_id
   *  AND ST_CONTAINS(ST_BUFFER(
   * 		ST_SETSRID(ST_MAKEPOINT(13.431947, 48.566736),4326)::geography, 500
   * 	)::geometry, d.location::geometry);
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status bikesInLocation(Query query) {
    String sql = "";
    Sensor gpsSensor = query.getGpsSensor();
    sql =
        "WITH data AS (\n"
            + "\tSELECT DISTINCT ON (bike_id) bike_id, %s AS location FROM %s t\n"
            + "\tGROUP BY bike_id, time, %s\n"
            + "\tORDER BY bike_id, time DESC\n"
            + ") SELECT b.bike_id, b.owner_name, d.location from data d, bikes b \n"
            + "WHERE b.bike_id = d.bike_id\n"
            + "AND ST_CONTAINS(ST_BUFFER(\n"
            + "\t\tST_SETSRID(ST_MAKEPOINT(%f, %f),4326)::geography, %d\n"
            + "\t)::geometry, d.location::geometry);";
    sql =
        String.format(
            Locale.US,
            sql,
            gpsSensor.getName(),
            tableName,
            gpsSensor.getName(),
            Constants.SPAWN_POINT.getLongitude(),
            Constants.SPAWN_POINT.getLatitude(),
            config.RADIUS);
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
    try (Statement statement = connection.createStatement()) {
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
    sqlBuilder
        .append("CREATE TABLE ")
        .append(tableName)
        .append(" (time TIMESTAMPTZ NOT NULL, ")
        .append("bike_id VARCHAR(20) REFERENCES bikes (bike_id)");
    config.SENSORS.forEach(
        sensor ->
            sqlBuilder
                .append(", ")
                .append(sensor.getName())
                .append(" ")
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
      sqlBuilder
          .append("('")
          .append(bike.getName())
          .append("', ")
          .append("'")
          .append(bike.getOwnerName())
          .append("')");
    }
    sqlBuilder.append(";");
    return sqlBuilder.toString();
  }

  /*
   * Returns an SQL statement, which inserts a batch of points into single wide table.
   */
  private String getInsertOneWideBatchSql(Batch batch) {
    Map<Long, List<String>> rows = batch.transform();
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
}
