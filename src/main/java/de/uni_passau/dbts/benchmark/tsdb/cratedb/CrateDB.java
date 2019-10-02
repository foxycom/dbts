package de.uni_passau.dbts.benchmark.tsdb.cratedb;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.tsdb.memsql.MemSQL;
import de.uni_passau.dbts.benchmark.utils.NameGenerator;
import de.uni_passau.dbts.benchmark.utils.SqlBuilder;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

/**
 * Implementation of benchmark scenarios for CrateDB.
 */
public class CrateDB implements Database {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MemSQL.class);

  /** 'Drop table' template. */
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s;";

  /** Name of the data table. */
  private static String tableName;

  /** Configuration singleton. */
  private static Config config;

  /** JDBC connection. */
  private Connection connection;

  /** SQL statements builder. */
  private SqlBuilder sqlBuilder;

  /** Random names generator. */
  private NameGenerator nameGenerator;

  /** Scenario templates file. */
  private STGroupFile scenarioTemplates;

  /** Creates an instance of the database controller. */
  public CrateDB() {
    sqlBuilder = new SqlBuilder();
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
    nameGenerator = NameGenerator.INSTANCE;
    scenarioTemplates = new STGroupFile("../templates/cratedb/scenarios.stg");
  }

  @Override
  public void init() throws TsdbException {
    try {
      connection =
          DriverManager.getConnection(
              String.format(Constants.CRATE_URL, config.HOST, config.PORT),
              Constants.CRATE_USER,
              Constants.CRATE_PASSWD);
    } catch (Exception e) {
      LOGGER.error("CrateDB could not be initialized because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      statement.addBatch(String.format(DROP_TABLE, "bikes"));
      statement.addBatch(String.format(DROP_TABLE, tableName));
      statement.executeBatch();
      connection.commit();

      LOGGER.info("Waiting {}ms until old data has been erased.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (SQLException e) {
      LOGGER.warn("An error occurred while erasing data in CrateDB because: {}", e.getMessage());
      LOGGER.warn(e.getNextException().getMessage());

      if (!e.getMessage().contains("does not exist")) {
        throw new TsdbException(e);
      }
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage());
    }
  }

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
   * {@inheritDoc} Maps the data schema concepts:
   *
   * <ul>
   *   <li>Bike name -&gt; A column in the table
   *   <li>Sensor names -&gt; Columns in the table
   *   <li>Sensor values -&gt; Rows in the table
   * </ul>
   *
   * <p><code>
   * CREATE TABLE test (time TIMESTAMPTZ NOT NULL, bike_id TEXT,
   * s_0 DOUBLE PRECISION, ..., s_12 GEO_POINT, ..., s_41 DOUBLE PRECISION);
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
   * {@inheritDoc} The batch is inserted with a single SQL query:
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., POINT(13.45173242391854 48.57619798343793), ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., POINT(13.45173242391754 48.57619798343709), ..., 65.78), ...
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT time, bike_id, s_33 FROM test WHERE (bike_id = 'bike_0') AND (time = '2018-08-30 02:00:00.0');
   * </code>
   */
  @Override
  public Status precisePoint(Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    Timestamp timestamp = new Timestamp(query.getStartTimestamp());
    ST template = scenarioTemplates.getInstanceOf("precisePoint");
    template.add("tableName", tableName).add("sensor", sensor.getName())
        .add("bike", bike.getName()).add("time", timestamp);
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT b.owner_name, l.bike_id, last_timestamp, t.s_12 FROM (
   *  SELECT MAX("time") OVER (PARTITION BY bike_id) AS last_timestamp, bike_id FROM test
   *   ) AS l, test t, bikes b
   *   WHERE l.bike_id = t.bike_id AND l.last_timestamp = t."time" AND b.bike_id = t.bike_id
   *  GROUP BY l.bike_id, b.owner_name, l.last_timestamp, t.s_12;
   * </code>
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = scenarioTemplates.getInstanceOf("lastKnownPosition");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor);
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT date_trunc('second', "time") as second,
   *  longitude(s_12) as longitude, latitude(s_12) as latitude,
   *  t.bike_id, b.owner_name
   *  FROM test t, bikes b
   *  WHERE b.bike_id = t.bike_id
   *  AND t.bike_id = 'bike_2'
   *  AND time &gt; '2018-08-30 02:00:00.0'
   *  AND time &lt; '2018-08-30 03:00:00.0'
   *  GROUP BY b.owner_name, t.bike_id, longitude, latitude, second
   *  ORDER BY second;
   * </code>
   */
  @Override
  public Status gpsPathScan(Query query) {
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    Sensor gpsSensor = query.getGpsSensor();
    Bike bike = query.getBikes().get(0);
    ST template = scenarioTemplates.getInstanceOf("gpsPathScan");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName())
        .add("bike", bike.getName()).add("start", startTimestamp)
        .add("end", endTimestamp);
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT d.second, d.bike_id, b.owner_name, longitude, latitude
   *  FROM bikes b, (
   *   SELECT date_trunc('second', "time") AS SECOND,
   * 	 bike_id, longitude(s_12) as longitude, latitude(s_12) as latitude
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_2'
   * 	 AND time &gt; '2018-08-30 02:00:00.0'
   * 	 AND time &lt; '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, longitude(s_12), latitude(s_12)
   * 	 HAVING AVG(s_17) &gt; 1000.000000
   *  ) d
   *  WHERE d.bike_id = b.bike_id
   *  ORDER BY d.second ASC, d.bike_id;
   * </code>
   */
  @Override
  public Status identifyTrips(Query query) {
    Bike bike = query.getBikes().get(0);
    Timestamp startTimestamp = new Timestamp(Constants.START_TIMESTAMP);
    Timestamp endTimestamp = new Timestamp(Constants.START_TIMESTAMP + config.QUERY_INTERVAL);
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    ST template = scenarioTemplates.getInstanceOf("identifyTrips");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName())
        .add("bike", bike.getName()).add("sensor", sensor.getName())
        .add("threshold", query.getThreshold()).add("start", startTimestamp)
        .add("end", endTimestamp);

    return executeQuery(template.render());
  }

  /**
   * CrateDB does not support distance aggregations.
   *
   * @param query The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status distanceDriven(Query query) {
    return new Status(false, 0, new NotImplementedException(""),
        "CrateDB does not support distance aggregation.");
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   * 	 SELECT DISTINCT(bike_id) FROM test t WHERE time &gt; '2018-08-30 03:00:00.0'
   *  );
   * </code>
   */
  @Override
  public Status offlineBikes(Query query) {
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    ST template = scenarioTemplates.getInstanceOf("offlineBikes");
    template.add("tableName", tableName).add("time", endTimestamp);
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT l.bike_id, MAX(l.minute) as last_time FROM test t, (
   *   SELECT date_trunc('minute', "time") as minute, bike_id
   *   FROM test t WHERE time &gt; '2018-08-30 03:00:00.0'
   *   GROUP BY minute, bike_id
   *   HAVING AVG(s_17) &gt; 1000.000000
   *  ) as l
   *  WHERE l.bike_id = t.bike_id
   *  GROUP BY l.bike_id;
   * </code>
   */
  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();

    Timestamp timestamp = new Timestamp(query.getEndTimestamp());
    ST template = scenarioTemplates.getInstanceOf("lastTimeActivelyDriven");
    template.add("tableName", tableName).add("sensor", sensor.getName())
        .add("threshold", query.getThreshold()).add("time", timestamp);
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT d.minute, b.bike_id, b.owner_name, d.value
   *  FROM (
   *  SELECT date_trunc('minute', "time") AS minute, bike_id, AVG(s_27) AS value
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_2'
   * 	 AND time &gt; '2018-08-30 02:00:00.0'
   * 	 AND time &lt; '2018-08-30 03:00:00.0'
   * 	 GROUP BY bike_id, minute
   *  ) d, bikes b WHERE b.bike_id = d.bike_id
   *  ORDER BY d.minute, b.bike_id;
   * </code>
   */
  @Override
  public Status downsample(Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    ST template = scenarioTemplates.getInstanceOf("downsample");
    template.add("tableName", tableName).add("sensor", sensor.getName())
        .add("bike", bike.getName()).add("start", startTimestamp)
        .add("end", endTimestamp);

    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT longitude(s_12) as longitude, latitude(s_12) as latitude, AVG(s_34)
   *  FROM test t
   *  WHERE time &gt; '2018-08-30 02:00:00.0' AND time &lt; '2018-08-30 03:00:00.0'
   *  GROUP BY longitude, latitude order by longitude, latitude;
   * </code>
   */
  @Override
  public Status airPollutionHeatMap(Query query) {
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    ST template = scenarioTemplates.getInstanceOf("airPollutionHeatMap");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor)
        .add("sensor", sensor.getName()).add("start", startTimestamp)
        .add("end", endTimestamp);
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
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
   */
  @Override
  public Status bikesInLocation(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = scenarioTemplates.getInstanceOf("bikesInLocation");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName());
    return executeQuery(template.render());
  }

  /**
   * Executes the SQL query and measures the execution time.
   *
   * @param sql SQL query.
   * @return Status of the execution.
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

  /**
   * Returns an SQL query, which creates a wide table for all sensors readings.
   *
   * <p><code>
   *   CREATE TABLE IF NOT EXISTS test (time TIMESTAMPTZ NOT NULL, bike_id TEXT, s_0 DOUBLE PRECISION,
   *   ..., s_12 GEO_POINT, ...);
   * </code></p>
   *
   * @return SQL query.
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

  /**
   * Returns an SQL query that inserts bikes meta data in batch.
   *
   * <p><code>
   *   INSERT INTO bikes (bike_id, owner_name) VALUES ('bike_0', 'John'), ('bike_1', 'Melissa'), ...
   * </code></p>
   *
   * @param bikesList List of bikes.
   * @return SQL query.
   */
  private String getInsertBikesSql(List<Bike> bikesList) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("INSERT INTO bikes (bike_id, owner_name) VALUES ");

    boolean firstIteration = true;
    for (Bike bike : bikesList) {
      if (firstIteration) {
        firstIteration = false;
      } else {
        sqlBuilder.append(", ");
      }

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

  /**
   * Returns an SQL query that inserts a batch of points into single wide table.
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., POINT(13.45173242391854 48.57619798343793), ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., POINT(13.45173242391754 48.57619798343709), ..., 65.78), ...
   * </code>
   *
   * @param batch Batch of points.
   * @return SQL query.
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
    String debug = sqlBuilder.toString();
    return sqlBuilder.toString();
  }
}
