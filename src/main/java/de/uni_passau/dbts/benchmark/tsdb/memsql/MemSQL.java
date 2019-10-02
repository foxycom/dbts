package de.uni_passau.dbts.benchmark.tsdb.memsql;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
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
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

/**
 * Implementation of benchmark scenarios for MemSQL.
 */
public class MemSQL implements Database {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemSQL.class);

  /** SQL template for dropping a table. */
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s;";

  /** Name of the table to use for synthetic data. */
  private static String tableName;

  /** Configuration singleton. */
  private static Config config;

  /** JDBC connection. */
  private Connection connection;

  /** Builder of SQL queries. */
  private SqlBuilder sqlBuilder;

  /** File containing SQL queries templates. */
  private STGroupFile templatesFile;

  /** Generator of human names. */
  private NameGenerator nameGenerator;

  /** Creates an instance of the database controller. */
  public MemSQL() {
    sqlBuilder = new SqlBuilder();
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
    templatesFile = new STGroupFile("templates/memsql/scenarios.stg");
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
      Class.forName(Constants.MYSQL_DRIVER_NAME);
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
      LOGGER.warn("delete old data table {} failed, because: {}", tableName, e.getMessage());
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
   * <p>While bikes meta data is stored in memory as a row-oriented table, the synthetic data
   * must be stored on disk as a column-oriented table:</p>
   * <p><code>
   *   CREATE TABLE test (time DATETIME(6) NOT NULL, bike_id VARCHAR(20), s_0 DOUBLE PRECISION,
   *   ..., s_12 GEOGRAPHYPOINT, ..., SHARD(bike_id), KEY(time) USING CLUSTERED COLUMNSTORE);
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
      LOGGER.error("Can't create MemSQL table because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  /**
   * Returns the size of the benchmarked database in GB.
   *
   * @return The size of the benchmarked database, i.e., 'test'.
   * @throws TsdbException If an error occurs while executing the query.
   */
  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  /**
   * {@inheritDoc} The batch is inserted using a single SQL query:
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., 'POINT(13.45173242391854 48.57619798343793)', ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., 'POINT(13.45173242391754 48.57619798343709)', ..., 65.78), ...
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
   *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_8') AND (time = '2018-08-29 18:00:00.0');
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT LAST_VALUE(t.s_12) OVER(PARTITION BY t.bike_id ORDER BY (time)),
   * MAX(time), t.bike_id, b.owner_name FROM test t, bikes b
   * WHERE t.bike_id = b.bike_id
   * GROUP BY t.bike_id;
   * </code>
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("lastKnownPosition");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName());
    return executeQuery(template.render());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT s_12 AS location, t.bike_id, b.owner_name FROM test t, bikes b
   * 	WHERE b.bike_id = t.bike_id
   * 	AND t.bike_id = 'bike_5'
   * 	AND time &gt; '2018-08-30 02:00:00.0'
   * 	AND time &lt; '2018-08-30 03:00:00.0';
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  WITH data AS (
   * 	 SELECT from_unixtime(unix_timestamp(time) DIV 1 * 1) AS SECOND,
   * 	 bike_id, s_12
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_8'
   * 	 AND time &gt; '2018-08-30 02:00:00.0'
   * 	 AND time &lt; '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, s_12
   * 	 HAVING AVG(s_17) &gt; 1000.000000
   * )
   * SELECT d.second, d.bike_id, b.owner_name, d.s_12 FROM bikes b, data d
   * WHERE d.bike_id = b.bike_id
   * ORDER BY d.second ASC, d.bike_id;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * WITH data AS (
   * 	SELECT FROM_unixtime(unix_timestamp(time) DIV 1 * 1) AS second, bike_id, s_12
   * 	FROM test t
   * 	WHERE bike_id = 'bike_2'
   * 	AND time &gt;= '2018-08-30 02:00:00.0' AND time &lt;= '2018-08-30 03:00:00.0'
   * 	GROUP BY second, bike_id, s_12
   * 	HAVING AVG(s_17) &gt; 1000.0
   * 	ORDER BY second
   * )
   * SELECT d.bike_id, b.owner_name,
   * GEOGRAPHY_LENGTH(
   *     CONCAT('LINESTRING(', GROUP_CONCAT(CONCAT(GEOGRAPHY_LONGITUDE(s_12), ' ', GEOGRAPHY_LATITUDE(s_12))), ')')
   * ) FROM data d, bikes b
   * WHERE d.bike_id = b.bike_id
   * GROUP BY d.bike_id;
   * </code>
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
    ST template = templatesFile.getInstanceOf("offlineBikes");
    template.add("tableName", tableName).add("time", endTimestamp);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * WITH last_trip AS (
   * SELECT from_unixtime(unix_timestamp(time) DIV 60 * 60) as minute, bike_id
   * FROM test t WHERE time &gt; '2018-08-30 03:00:00.0'
   * GROUP BY minute, bike_id
   * HAVING AVG(s_17) &gt; 1000.0
   * ) SELECT l.bike_id, MAX(l.minute) as last_time FROM test t, last_trip l
   * WHERE l.bike_id = t.bike_id
   * GROUP BY l.bike_id;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  WITH downsample AS (
   * 	 SELECT from_unixtime(unix_timestamp(time) DIV 60 * 60) AS minute, bike_id, AVG(s_27) AS value
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_8'
   * 	 AND time &gt; '2018-08-30 02:00:00.0'
   * 	 AND time &lt; '2018-08-30 03:00:00.0'
   * 	 GROUP BY bike_id, minute
   *  ) SELECT d.minute, b.bike_id, b.owner_name, d.value
   *  FROM downsample d, bikes b WHERE b.bike_id = d.bike_id
   *  ORDER BY d.minute, b.bike_id;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT GEOGRAPHY_LONGITUDE(s_12) as longitude, GEOGRAPHY_LATITUDE(s_12) as latitude, AVG(s_34)
   *  FROM test t
   *  WHERE time &gt; '2018-08-30 02:00:00.0' AND time &lt; '2018-08-30 03:00:00.0'
   *  GROUP BY longitude, latitude;
   * </code>
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
   * {@inheritDoc} Example query:
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
   */
  @Override
  public Status bikesInLocation(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("bikesInLocation");
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
   * Returns an SQL query, which creates a wide column-oriented table for all sensors readings.
   *
   * <p><code>
   *   CREATE TABLE test (time DATETIME(6) NOT NULL, bike_id VARCHAR(20), s_0 DOUBLE PRECISION,
   *   ..., s_12 GEOGRAPHYPOINT, ..., SHARD(bike_id), KEY(time) USING CLUSTERED COLUMNSTORE);
   * </code></p>
   *
   * @return SQL query.
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

  /**
   * Returns an SQL query that insert bikes meta data in batch.
   *
   * <p><code>
   *   INSERT INTO bikes (bike_id, owner_name) VALUES ('bike_0', 'John'), ('bike_1', 'Melissa');
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
   * Returns an SQL query, which inserts a batch of points into a single wide table.
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., 'POINT(13.45173242391854 48.57619798343793)', ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., 'POINT(13.45173242391754 48.57619798343709)', ..., 65.78), ...
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
    return sqlBuilder.toString();
  }
}
