package cn.edu.tsinghua.iotdb.benchmark.tsdb.memsql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.NameGenerator;
import cn.edu.tsinghua.iotdb.benchmark.utils.SqlBuilder;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.sql.*;
import java.util.*;

public class MemSQL implements IDatabase {

  private static final int B2GB = 1024 * 1024 * 1024;
  private Connection connection;
  private static String tableName;
  private static Config config;
  private static final Logger LOGGER = LoggerFactory.getLogger(MemSQL.class);
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s;";
  private long initialDbSize;
  private SqlBuilder sqlBuilder;
  private STGroupFile templatesFile;
  private NameGenerator nameGenerator;

  /** Initializes an instance of the database controller. */
  public MemSQL() {
    sqlBuilder = new SqlBuilder();
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
    templatesFile = new STGroupFile("memsql/scenarios.stg");
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
      Class.forName(Constants.MYSQL_DRIVENAME);
      connection =
          DriverManager.getConnection(
              String.format(Constants.MYSQL_URL, config.HOST, config.PORT, config.DB_NAME),
              Constants.MYSQL_USER,
              Constants.MYSQL_PASSWD);
    } catch (Exception e) {
      LOGGER.error("Initialize TimescaleDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  /*
   * Returns the size of a given database in bytes.
   */
  private long getInitialSize() {
    return 0;
  }

  /**
   * Erases the data from the database by dropping benchmark tables.
   *
   * @throws TsdbException If an error occurs while cleaning up.
   */
  @Override
  public void cleanup() throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.addBatch(String.format(DROP_TABLE, "bikes"));
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

      // Creates bikes' relational data table.
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS bikes (bike_id VARCHAR(20), owner_name VARCHAR(100) NOT NULL, "
              + "PRIMARY KEY (bike_id));");

      // Inserts all bikes.
      statement.addBatch(getInsertBikesSql(schemaList));

      statement.addBatch(getCreateTableSql());
      statement.executeBatch();
      connection.commit();
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

      String insertBatchSql = getInsertOneBatchSql(batch);
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
   * <p>NARROW_TABLE:
   *
   * <p><code>
   *  SELECT value, time, bike_id FROM emg_benchmark WHERE (bike_id = 'bike_10') AND (time = '2018-08-29 18:00:00.0');
   * </code> WIDE_TABLE:
   *
   * <p><code>
   *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_8') AND (time = '2018-08-29 18:00:00.0');
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
    return executeQuery(sqlBuilder.build());
  }

  /**
   * Selects the last known GPS position of bikes.
   *
   * <p><code>
   * SELECT LAST_VALUE(t.s_12) OVER(PARTITION BY t.bike_id ORDER BY (time)),
   * MAX(time), t.bike_id, b.owner_name FROM test t, bikes b
   * WHERE t.bike_id = b.bike_id
   * GROUP BY t.bike_id;
   * </code>
   *
   * @param query The query parameters query.
   * @return The status of the execution.
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("lastKnownPosition");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName());
    return executeQuery(template.render());
  }

  /**
   * Performs a scan of gps data points in a given time range for specified number of bikes.
   *
   * <p>NARROW_TABLE:
   *
   * <p><code>
   *  SELECT time, bike_id, value FROM gps_benchmark WHERE (bike_id = 'bike_10') AND (time >= '2018-08-29 18:00:00.0'
   *  AND time <= '2018-08-29 19:00:00.0');
   * </code> WIDE_TABLE:
   *
   * <p><code>
   *  SELECT s_12 AS location, t.bike_id, b.owner_name FROM test t, bikes b
   * 	WHERE b.bike_id = t.bike_id
   * 	AND t.bike_id = 'bike_5'
   * 	AND time >= '2018-08-30 02:00:00.0'
   * 	AND time <= '2018-08-30 03:00:00.0';
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
    ST template = templatesFile.getInstanceOf("gpsPathScan");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("bike", bike.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp);
    return executeQuery(template.render());
  }

  /**
   * Identifies active trips, when current value is above a threshold.
   *
   * <p><code>
   *  WITH data AS (
   * 	 SELECT from_unixtime(unix_timestamp(time) DIV 1 * 1) AS SECOND,
   * 	 bike_id, s_12
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_8'
   * 	 AND time >= '2018-08-30 02:00:00.0'
   * 	 AND time < '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, s_12
   * 	 HAVING AVG(s_17) >= 1000.000000
   * )
   * SELECT d.second, d.bike_id, b.owner_name, d.s_12 FROM bikes b, data d
   * WHERE d.bike_id = b.bike_id
   * ORDER BY d.second ASC, d.bike_id;
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
    ST template = templatesFile.getInstanceOf("identifyTrips");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("sensor", sensor.getName())
        .add("threshold", query.getThreshold())
        .add("start", startTimestamp)
        .add("end", endTimestamp)
        .add("bike", bike.getName());
    return executeQuery(template.render());
  }

  /**
   * Computes the distance driven by a bike in the given time range identified by start and end
   * timestamps where current exceeds some value.
   *
   * <p><code>
   * WITH data AS (
   * 	SELECT FROM_unixtime(unix_timestamp(time) DIV 1 * 1) AS second, bike_id, s_12
   * 	FROM test t
   * 	WHERE bike_id = 'bike_2'
   * 	AND time >= '2018-08-30 02:00:00.0' AND time <= '2018-08-30 03:00:00.0'
   * 	GROUP BY second, bike_id, s_12
   * 	HAVING AVG(s_17) > 1000.0
   * 	ORDER BY second
   * )
   * SELECT d.bike_id, b.owner_name,
   * GEOGRAPHY_LENGTH(
   *     CONCAT('LINESTRING(', GROUP_CONCAT(CONCAT(GEOGRAPHY_LONGITUDE(s_12), ' ', GEOGRAPHY_LATITUDE(s_12))), ')')
   * ) FROM data d, bikes b
   * WHERE d.bike_id = b.bike_id
   * GROUP BY d.bike_id;
   * </code>
   *
   * @param query The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status distanceDriven(Query query) {
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    Bike bike = query.getBikes().get(0);
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("distanceDriven");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("sensor", sensor.getName())
        .add("threshold", query.getThreshold())
        .add("bike", bike.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp);
    return executeQuery(template.render());
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
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    ST template = templatesFile.getInstanceOf("offlineBikes");
    template.add("tableName", tableName).add("time", endTimestamp);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * TODO probably include also gps coordinates or path segment Scans metrics of a sensor in a
   * specific time interval and selects those which have a certain value.
   *
   * <p>NARROW_TABLE:
   *
   * <p><code>
   *  SELECT time, bike_id, value FROM emg_benchmark WHERE value > 3.0 AND (bike_id = 'bike_5')
   *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
   * </code> WIDE_TABLE:
   *
   * <p><code>
   * SELECT data.minute, data.bike_id, b.owner_name FROM bikes b INNER JOIN LATERAL (
   * 	SELECT time_bucket(interval '1 min', time) AS minute, bike_id FROM test t
   * 	WHERE t.bike_id = b.bike_id AND time > '2018-08-29 18:00:00.0'
   * 	AND s_17 IS NOT NULL
   * 	GROUP BY minute, bike_id
   * 	HAVING AVG(s_17) > 3.000000
   * 	ORDER BY minute DESC LIMIT 1
   *  ) AS data
   *  ON true
   *  ORDER BY b.bike_id, data.minute DESC;
   * </code>
   *
   * @param query The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();
    Timestamp timestamp = new Timestamp(query.getEndTimestamp());
    ST template = templatesFile.getInstanceOf("lastTimeActivelyDriven");
    template
        .add("tableName", tableName)
        .add("time", timestamp)
        .add("sensor", sensor.getName())
        .add("threshold", query.getThreshold());

    return executeQuery(template.render());
  }

  /**
   * Groups entries within a time range in time buckets and by bikes and aggregates values in each
   * time bucket.
   *
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
    ST template = templatesFile.getInstanceOf("downsample");
    template
        .add("tableName", tableName)
        .add("sensor", sensor.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp)
        .add("bike", bike.getName());
    return executeQuery(template.render());
  }

  /**
   * Creates a heat map with average air quality out of gps points.
   *
   * <p><code>
   *  SELECT GEOGRAPHY_LONGITUDE(s_12) as longitude, GEOGRAPHY_LATITUDE(s_12) as latitude, AVG(s_34)
   *  FROM test t
   *  WHERE s_12 IS NOT NULL AND time >= '2018-08-30 02:00:00.0' AND time <= '2018-08-30 03:00:00.0'
   *  GROUP BY s_12;
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
    ST template = templatesFile.getInstanceOf("airPollutionHeatMap");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("sensor", sensor.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp);
    return executeQuery(template.render());
  }

  /**
   * Selects bikes whose last gps location lies in a certain area.
   *
   * <p><code>
   * SELECT b.bike_id, b.owner_name, pos.pos FROM bikes b,
   * 	(SELECT bike_id, LAST_VALUE(s_12) OVER (PARTITION BY bike_id ORDER BY (time)) AS pos FROM test
   * 	GROUP BY bike_id) AS pos
   * WHERE b.bike_id = pos.bike_id
   * AND GEOGRAPHY_CONTAINS('POLYGON((13.4406567 48.5723195,
   * 13.4373522 48.5707861, 13.4373522 48.5662708,
   * 13.4443045 48.5645384, 13.4489393 48.5683155,
   * 13.4492826 48.5710701, 13.4406567 48.5723195))', pos.pos);
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status bikesInLocation(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("bikesInLocation");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName());
    return executeQuery(template.render());
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
        .append(" (time DATETIME(6) NOT NULL, ")
        .append("bike_id VARCHAR(20)");
    config.SENSORS.forEach(
        sensor ->
            sqlBuilder
                .append(", ")
                .append(sensor.getName())
                .append(" ")
                .append(sensor.getDataType()));
    sqlBuilder.append(", SHARD(bike_id), KEY(time) USING CLUSTERED COLUMNSTORE)");
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
  private String getInsertOneBatchSql(Batch batch) {
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
