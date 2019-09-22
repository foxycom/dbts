package cn.edu.tsinghua.iotdb.benchmark.tsdb.cratedb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.NameGenerator;
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
  private static final Logger LOGGER =
      LoggerFactory.getLogger(cn.edu.tsinghua.iotdb.benchmark.tsdb.memsql.MemSQL.class);
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s;";
  private SqlBuilder sqlBuilder;
  private NameGenerator nameGenerator;

  /** Initializes an instance of the database controller. */
  public CrateDB() {
    sqlBuilder = new SqlBuilder();
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
    nameGenerator = NameGenerator.INSTANCE;
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
              String.format(Constants.CRATE_URL, config.HOST, config.PORT),
              Constants.CRATE_USER,
              Constants.CRATE_PASSWD);
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
    // delete old data
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

      // Creates bikes' relational data table.
      statement.execute(
          "CREATE TABLE IF NOT EXISTS bikes (bike_id TEXT, owner_name TEXT NOT NULL, "
              + "PRIMARY KEY (bike_id));");

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
   *  SELECT time, bike_id, s_33 FROM test WHERE (bike_id = 'bike_0') AND (time = '2018-08-30 02:00:00.0');
   * </code>
   *
   * @param query The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status precisePoint(Query query) {
    Sensor sensor = query.getSensor();
    long timestamp = query.getStartTimestamp();

    List<String> columns =
        Arrays.asList(
            SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName(), sensor.getName());
    sqlBuilder =
        sqlBuilder
            .reset()
            .select(columns)
            .from(tableName)
            .where()
            .bikes(query.getBikes())
            .and()
            .time(timestamp);
    String debug = sqlBuilder.build();
    return executeQuery(sqlBuilder.build());
  }

  /**
   * Selects the last known GPS position of bikes.
   *
   * <p><code>
   *  SELECT b.owner_name, l.bike_id, last_timestamp, t.s_12 FROM (
   *  SELECT MAX("time") OVER (PARTITION BY bike_id) AS last_timestamp, bike_id FROM test
   *   ) AS l, test t, bikes b
   *   WHERE l.bike_id = t.bike_id AND l.last_timestamp = t."time" AND b.bike_id = t.bike_id
   *  GROUP BY l.bike_id, b.owner_name, l.last_timestamp, t.s_12;
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
        "SELECT b.owner_name, l.bike_id, last_timestamp, t.%s FROM (\n"
            + "SELECT MAX(\"time\") OVER (PARTITION BY bike_id) AS last_timestamp, bike_id FROM %s\n"
            + "  ) AS l, %s t, bikes b\n"
            + "  WHERE l.bike_id = t.bike_id AND l.last_timestamp = t.\"time\" AND b.bike_id = t.bike_id\n"
            + "GROUP BY l.bike_id, b.owner_name, l.last_timestamp, t.%s;";
    sql =
        String.format(
            Locale.US, sql, gpsSensor.getName(), tableName, tableName, gpsSensor.getName());

    return executeQuery(sql);
  }

  /**
   * Performs a scan of gps data points in a given time range for specified number of bikes.
   *
   * <p><code>
   *  SELECT date_trunc('second', "time") as second,
   *  longitude(s_12) as longitude, latitude(s_12) as latitude,
   *  t.bike_id, b.owner_name
   *  FROM test t, bikes b
   *  WHERE b.bike_id = t.bike_id
   *  AND t.bike_id = 'bike_2'
   *  AND time >= '2018-08-30 02:00:00.0'
   *  AND time <= '2018-08-30 03:00:00.0'
   *  group by b.owner_name, t.bike_id, longitude, latitude, second
   *  ORDER BY second;
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
    String sql = "";

    sql =
        "SELECT date_trunc('second', \"time\") as second, \n"
            + "longitude(%s) as longitude, latitude(%s) as latitude, \n"
            + "t.bike_id, b.owner_name \n"
            + "FROM %s t, bikes b\n"
            + "WHERE b.bike_id = t.bike_id\n"
            + "AND t.bike_id = '%s'\n"
            + "AND time >= '%s' \n"
            + "AND time <= '%s'\n"
            + "group by b.owner_name, t.bike_id, longitude, latitude, second\n"
            + "ORDER BY second;";
    sql =
        String.format(
            Locale.US,
            sql,
            gpsSensor.getName(),
            gpsSensor.getName(),
            tableName,
            bike.getName(),
            startTimestamp,
            endTimestamp);
    return executeQuery(sql);
  }

  /**
   * Identifies active trips, when current value is above a threshold.
   *
   * <p><code>
   *  SELECT d.second, d.bike_id, b.owner_name, longitude, latitude
   *  FROM bikes b, (
   *   SELECT date_trunc('second', "time") AS SECOND,
   * 	 bike_id, longitude(s_12) as longitude, latitude(s_12) as latitude
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_2'
   * 	 AND time >= '2018-08-30 02:00:00.0'
   * 	 AND time < '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, longitude(s_12), latitude(s_12)
   * 	 HAVING AVG(s_17) >= 1000.000000
   *  ) d
   *  WHERE d.bike_id = b.bike_id
   *  ORDER BY d.second ASC, d.bike_id;
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
        "SELECT d.second, d.bike_id, b.owner_name, longitude, latitude\n"
            + "FROM bikes b, (\n"
            + "  SELECT date_trunc('second', \"time\") AS SECOND, \n"
            + "\tbike_id, longitude(%s) as longitude, latitude(%s) as latitude\n"
            + "\tFROM %s t \n"
            + "\tWHERE bike_id = '%s'\n"
            + "\tAND time >= '%s' \n"
            + "\tAND time < '%s'\n"
            + "\tGROUP BY second, bike_id, longitude(%s), latitude(%s)\n"
            + "\tHAVING AVG(%s) >= %f\n"
            + ") d\n"
            + "WHERE d.bike_id = b.bike_id\n"
            + "ORDER BY d.second ASC, d.bike_id;";
    sql =
        String.format(
            Locale.US,
            sql,
            gpsSensor.getName(),
            gpsSensor.getName(),
            tableName,
            bike.getName(),
            startTimestamp,
            endTimestamp,
            gpsSensor.getName(),
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

    return new Status(false, 0);
  }

  /**
   * Selects bikes that did not send any data since a given timestamp.
   *
   * <p><code>
   *  SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   * 	 SELECT DISTINCT(bike_id) FROM test t WHERE time > '2018-08-30 03:00:00.0'
   *  );
   * </code>
   *
   * @param query The query params object.
   * @return Status of the execution.
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
   * TODO probably include also gps coordinates or path segment Scans metrics of a sensor in a
   * specific time interval and selects those which have a certain value.
   *
   * <p><code>
   *  SELECT l.bike_id, MAX(l.minute) as last_time FROM test t, (
   *   SELECT date_trunc('minute', "time") as minute, bike_id
   *   FROM test t WHERE time > '2018-08-30 03:00:00.0'
   *   GROUP BY minute, bike_id
   *   HAVING AVG(s_17) > 1000.000000
   *  ) as l
   *  WHERE l.bike_id = t.bike_id
   *  GROUP BY l.bike_id;
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
        "SELECT l.bike_id, MAX(l.minute) as last_time FROM %s t, (\n"
            + "SELECT date_trunc('minute', \"time\") as minute, bike_id\n"
            + "FROM %s t WHERE time > '%s'\n"
            + "GROUP BY minute, bike_id\n"
            + "HAVING AVG(%s) > %f  \n"
            + ") as l\n"
            + "WHERE l.bike_id = t.bike_id\n"
            + "GROUP BY l.bike_id;";
    sql =
        String.format(
            Locale.US,
            sql,
            tableName,
            tableName,
            timestamp,
            sensor.getName(),
            query.getThreshold());
    return executeQuery(sql);
  }

  /**
   * Groups entries within a time range in time buckets and by bikes and aggregates values in each
   * time bucket.
   *
   * <p><code>
   *  SELECT d.minute, b.bike_id, b.owner_name, d.value
   *  FROM (
   *  SELECT date_trunc('minute', "time") AS minute, bike_id, AVG(s_27) AS value
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_2'
   * 	 AND time >= '2018-08-30 02:00:00.0'
   * 	 AND time <= '2018-08-30 03:00:00.0'
   * 	 GROUP BY bike_id, minute
   *  ) d, bikes b WHERE b.bike_id = d.bike_id
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
        "SELECT d.minute, b.bike_id, b.owner_name, d.value \n"
            + "FROM (\n"
            + "SELECT date_trunc('minute', \"time\") AS minute, bike_id, AVG(%s) AS value\n"
            + "\tFROM %s t \n"
            + "\tWHERE bike_id = '%s'\n"
            + "\tAND time >= '%s'\n"
            + "\tAND time <= '%s'\n"
            + "\tGROUP BY bike_id, minute\n"
            + ") d, bikes b WHERE b.bike_id = d.bike_id\n"
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
   *  SELECT longitude(s_12) as longitude, latitude(s_12) as latitude, AVG(s_34)
   *  FROM test t
   *  WHERE time >= '2018-08-30 02:00:00.0' AND time <= '2018-08-30 03:00:00.0'
   *  GROUP BY longitude, latitude order by longitude, latitude;
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
        "SELECT longitude(%s) as longitude, latitude(%s) as latitude, AVG(%s) \n"
            + "FROM %s t \n"
            + "WHERE time >= '%s' AND time <= '%s' \n"
            + "GROUP BY longitude, latitude order by longitude, latitude;";
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
   *  SELECT b.bike_id, b.owner_name, pos.pos FROM bikes b,
   * 	(
   *     SELECT t.bike_id, t.s_12 AS pos FROM (
   *       SELECT MAX("time") OVER (PARTITION BY bike_id) AS last_timestamp, bike_id
   *       FROM test
   *     ) l, test t
   *     WHERE l.bike_id = t.bike_id
   *     AND l.last_timestamp = t."time"
   *   ) pos
   *  WHERE b.bike_id = pos.bike_id
   *  AND within(pos.pos, 'POLYGON((13.4406567 48.5723195,
   *  13.4373522 48.5707861, 13.4373522 48.5662708,
   *  13.4443045 48.5645384, 13.4489393 48.5683155,
   *  13.4492826 48.5710701, 13.4406567 48.5723195))');
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
        "SELECT b.bike_id, b.owner_name, pos.pos FROM bikes b, \n"
            + "\t(\n"
            + "SELECT t.bike_id, t.%s AS pos FROM (\n"
            + "SELECT MAX(\"time\") OVER (PARTITION BY bike_id) AS last_timestamp, bike_id\n"
            + "FROM %s\n"
            + ") l, %s t \n"
            + "WHERE l.bike_id = t.bike_id\n"
            + "AND l.last_timestamp = t.\"time\"\n"
            + ") pos \n"
            + "WHERE b.bike_id = pos.bike_id \n"
            + "AND within(pos.pos, 'POLYGON((13.4406567 48.5723195, \n"
            + "13.4373522 48.5707861, 13.4373522 48.5662708, \n"
            + "13.4443045 48.5645384, 13.4489393 48.5683155, \n"
            + "13.4492826 48.5710701, 13.4406567 48.5723195))');";
    sql = String.format(Locale.US, sql, gpsSensor.getName(), tableName, tableName);
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
        .append("CREATE TABLE IF NOT EXISTS ")
        .append(tableName)
        .append(" (time TIMESTAMPTZ NOT NULL, ")
        .append("bike_id TEXT");
    config.SENSORS.forEach(
        sensor ->
            sqlBuilder
                .append(", ")
                .append(sensor.getName())
                .append(" ")
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
      sqlBuilder
          .append("('")
          .append(bike.getName())
          .append("', ")
          .append("'")
          .append(nameGenerator.getName())
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
    String debug = sqlBuilder.toString();
    return sqlBuilder.toString();
  }
}
