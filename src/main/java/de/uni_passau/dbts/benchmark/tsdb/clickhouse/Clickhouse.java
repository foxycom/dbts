package de.uni_passau.dbts.benchmark.tsdb.clickhouse;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.utils.NameGenerator;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

public class Clickhouse implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(Clickhouse.class);

  /** SQL template for dropping a table. */
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s;";

  /** Configuration singleton. */
  private static Config config;

  /** JDBC connection. */
  private Connection connection;

  /** Generator of human names. */
  private NameGenerator nameGenerator;

  /** Name of the table to use for synthetic data. */
  private String tableName;

  /** File containing queries templates. */
  private STGroupFile templatesFile;

  /** Creates an instance of the database controller. */
  public Clickhouse() {
    config = ConfigParser.INSTANCE.config();
    nameGenerator = NameGenerator.INSTANCE;
    templatesFile = new STGroupFile("../templates/clickhouse/scenarios.stg");
    tableName = "test";
  }

  @Override
  public void init() throws TsdbException {
    try {
      Class.forName(Constants.CLICKHOUSE_DRIVER);
      connection =
          DriverManager.getConnection(
              String.format(Constants.CLICKHOUSE_URL, config.HOST, config.PORT, config.DB_NAME),
              Constants.CLICKHOUSE_USER,
              Constants.CLICKHOUSE_PASSWD);
    } catch (ClassNotFoundException | SQLException e) {
      LOGGER.error("Initialization of Clickhouse failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(String.format(DROP_TABLE, "bikes"));
      statement.execute(String.format(DROP_TABLE, tableName));

      LOGGER.info("Waiting {}ms to erase old data.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (SQLException e) {
      LOGGER.warn("Erasing {} failed, because: {}", tableName, e.getMessage());
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
    } catch (SQLException e) {
      LOGGER.debug("Could not close connection to Clickhouse because: {}", e.getMessage());
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
   * <p>The wide table with sensor readings is defined as:
   *
   * <p><code>
   * CREATE TABLE test (time UInt64, bike_id String, s_0 Nullable(Float64), ..., longitude Float64,
   * latitude Float64, ...)
   * ENGINE = MergeTree() PARTITION BY toYYYMMDD(toDate(time/1000)) ORDER BY (bike_id, time);
   * </code>
   *
   * <p>As <a href="https://clickhouse.yandex/docs/en/data_types/datetime/">DateTime</a> can only
   * store timestamps with a second precision, here milliseconds timestamps have to be store as
   * <code>UInt64</code> and converted to DateTime whenever needed.
   */
  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try (Statement statement = connection.createStatement()) {

      // Creates bikes' relational data table.
      statement.execute(
          "CREATE TABLE IF NOT EXISTS bikes (bike_id String, owner_name String) "
              + "ENGINE = MergeTree() ORDER BY bike_id;");

      // Inserts all bikes.
      statement.execute(getInsertBikesSql(schemaList));

      // Creates sensor readings schema.
      statement.execute(getCreateTableSql());

    } catch (SQLException e) {
      LOGGER.error("Can't create Clickhouse table because: {}", e.getMessage());
      System.out.println(e.getNextException());
      throw new TsdbException(e);
    }
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  /**
   * {@inheritDoc} The batch is inserted with a single SQL query:
   *
   * <p><code>
   *   INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., 13.45173242391854, 48.57619798343793, ..., 23.43),
   *   ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., 13.45173242391754, 48.57619798343709, ..., 65.78), ...
   * </code>
   */
  @Override
  public Status insertOneBatch(Batch batch) {
    long startTime;
    long endTime;
    try (Statement statement = connection.createStatement()) {
      String insertBatchSql = getInsertOneBatchSql(batch);
      startTime = System.nanoTime();
      statement.execute(insertBatchSql);
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
   * SELECT time, bike_id, s_23 FROM test WHERE time = 1535587300000 AND bike_id = 'bike_39';
   * </code>
   */
  @Override
  public Status precisePoint(Query query) {
    Bike bike = query.getBikes().get(0);
    Sensor sensor = query.getSensor();
    ST template = templatesFile.getInstanceOf("precisePoint");
    template
        .add("tableName", tableName)
        .add("time", query.getStartTimestamp())
        .add("bike", bike.getName())
        .add("sensor", sensor.getName());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT longitude, latitude, t.bike_id, b.owner_name FROM test AS t, bikes AS b
   * WHERE b.bike_id = t.bike_id
   * AND t.bike_id = 'bike_3'
   * AND time &gt; 1535587200000
   * AND time &lt; 1535587300000;
   * </code>
   */
  @Override
  public Status gpsPathScan(Query query) {
    Bike bike = query.getBikes().get(0);
    ST template = templatesFile.getInstanceOf("gpsPathScan");
    template
        .add("tableName", tableName)
        .add("bike", bike.getName())
        .add("start", query.getStartTimestamp())
        .add("end", query.getEndTimestamp());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT d.second, d.bike_id, b.owner_name, longitude, latitude FROM bikes as b, (
   * 	SELECT toUInt64(time/1000) AS second,
   * 	bike_id, longitude, latitude
   * 	FROM test t
   * 	WHERE bike_id = 'bike_2'
   * 	AND time &gt; 1535587200000
   * 	AND time &lt; 1535587300000
   * 	GROUP BY second, bike_id, longitude, latitude
   * 	HAVING AVG(s_17) &gt; 1000.0
   * ) as d
   * WHERE d.bike_id = b.bike_id
   * ORDER BY d.second ASC, d.bike_id;
   * </code>
   */
  @Override
  public Status identifyTrips(Query query) {
    Bike bike = query.getBikes().get(0);
    Sensor sensor = query.getSensor();
    ST template = templatesFile.getInstanceOf("identifyTrips");
    template
        .add("tableName", tableName)
        .add("bike", bike.getName())
        .add("start", query.getStartTimestamp())
        .add("end", query.getEndTimestamp())
        .add("sensor", sensor.getName())
        .add("threshold", query.getThreshold());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   * 	SELECT DISTINCT(bike_id) FROM test WHERE time &gt; 1535587300000
   * );
   * </code>
   */
  @Override
  public Status offlineBikes(Query query) {
    ST template = templatesFile.getInstanceOf("offlineBikes");
    template.add("tableName", tableName).add("time", query.getEndTimestamp());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT MAX(minute) as last_time, bike_id from (
   *     SELECT toStartOfMinute(toDateTime(toInt64(time/1000))) AS minute, bike_id
   *     FROM test
   *     WHERE time &gt; 1535587201000
   *     GROUP BY minute, bike_id HAVING AVG(s_32) &gt; 1000.0
   * ) GROUP BY bike_id;
   * </code>
   */
  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();
    ST template = templatesFile.getInstanceOf("lastTimeActivelyDriven");
    template
        .add("tableName", tableName)
        .add("time", query.getEndTimestamp())
        .add("sensor", sensor.getName())
        .add("threshold", query.getThreshold());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT d.minute, b.bike_id, b.owner_name, d.value
   * FROM (
   *     SELECT toStartOfMinute(toDateTime(time/1000)) AS minute, bike_id, AVG(s_27) AS value
   * 	FROM test AS t
   * 	WHERE bike_id = 'bike_2'
   * 	AND time &gt; 1535587200000
   * 	AND time &lt; 1535587300000
   * 	GROUP BY bike_id, minute
   * ) AS d, bikes AS b WHERE b.bike_id = d.bike_id
   * ORDER BY d.minute, b.bike_id;
   * </code>
   */
  @Override
  public Status downsample(Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    ST template = templatesFile.getInstanceOf("downsample");
    template
        .add("tableName", tableName)
        .add("sensor", sensor.getName())
        .add("bike", bike.getName())
        .add("start", query.getStartTimestamp())
        .add("end", query.getEndTimestamp());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT bike_id, last_time, longitude, latitude FROM (
   *     SELECT max(time) AS last_time, bike_id
   *     FROM test GROUP BY bike_id
   * ) AS d, test AS t
   * WHERE t.time = d.last_time AND t.bike_id = d.bike_id;
   * </code>
   */
  @Override
  public Status lastKnownPosition(Query query) {
    ST template = templatesFile.getInstanceOf("lastKnownPosition");
    template.add("tableName", tableName);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT longitude, latitude, AVG(s_34)
   * FROM test t
   * WHERE time &gt; 1535587200000 AND time &lt; 1535587300000
   * GROUP BY longitude, latitude;
   * </code>
   */
  @Override
  public Status airPollutionHeatMap(Query query) {
    Sensor sensor = query.getSensor();
    ST template = templatesFile.getInstanceOf("airPollutionHeatMap");
    template
        .add("tableName", tableName)
        .add("sensor", sensor.getName())
        .add("start", query.getStartTimestamp())
        .add("end", query.getEndTimestamp());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * ClickHouse does not support distance aggregations, yet. However, it can compute distance between
   * two geospatial points. Some custom function might be applied chronologically to every two rows
   * to compute distance and then summarize the results.
   *
   * TODO test self join with row numbers.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  @Override
  public Status distanceDriven(Query query) {
    throw new NotImplementedException("Distance aggregation is not implemented in ClickHouse.");
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT bike_id, owner_name, longitude, latitude FROM (
   *    SELECT max(time) AS last_time, bike_id FROM test GROUP BY bike_id
   * ) AS l, test AS t, bikes AS b
   * WHERE t.time = l.last_time AND t.bike_id = l.bike_id AND b.bike_id = t.bike_id
   * AND pointInPolygon((longitude, latitude), [(13.4406567, 48.5723195), (13.4373522, 48.5707861), (13.4373522, 48.5662708),
   * (13.4443045, 48.5645384), (13.4489393, 48.5683155), (13.4492826, 48.5710701), (13.4406567, 48.5723195)]);
   * </code>
   */
  @Override
  public Status bikesInLocation(Query query) {
    ST template = templatesFile.getInstanceOf("bikesInLocation");
    template.add("tableName", tableName)
        .add("longitude", Constants.SPAWN_POINT.getLongitude())
        .add("latitude", Constants.SPAWN_POINT.getLatitude())
        .add("radius", config.RADIUS);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * Returns an SQL query which insert bikes meta data as batch.
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
   * Returns an SQL query, which creates a wide table for all sensors readings.
   *
   * <p><code>
   * CREATE TABLE test (time UInt64, bike_id String, s_0 Nullable(Float64), ..., longitude Float64,
   * latitude Float64, ...)
   * ENGINE = MergeTree() PARTITION BY toYYYMMDD(toDate(time/1000)) ORDER BY (bike_id, time);
   * </code>
   *
   * @return SQL query.
   */
  private String getCreateTableSql() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(tableName)
        .append(" (time UInt64, ")
        .append("bike_id String");
    config.SENSORS.forEach(
        sensor -> {
          if (sensor.getFields().size() == 1) {
            sqlBuilder
                .append(", ")
                .append(sensor.getName())
                .append(" Nullable(")
                .append(sensor.getDataType())
                .append(")");
          } else {
            sensor
                .getFields()
                .forEach(
                    field ->
                        sqlBuilder
                            .append(", ")
                            .append(field)
                            .append(" ")
                            .append(sensor.getDataType()));
          }
        });
    sqlBuilder.append(
        ") ENGINE = MergeTree() PARTITION BY toYYYYMMDD(toDate(time/1000)) ORDER BY (bike_id, time);");
    return sqlBuilder.toString();
  }

  /**
   * Creates an SQL query for inserting a batch of points.
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., 13.45173242391854, 48.57619798343793, ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., 13.45173242391754, 48.57619798343709, ..., 65.78), ...
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
    for (long timestamp : rows.keySet()) {
      if (firstIteration) {
        firstIteration = false;
      } else {
        sqlBuilder.append(", ");
      }

      sqlBuilder.append("(").append(timestamp).append(", '").append(bike.getName()).append("'");
      List<String> valuesList = rows.get(timestamp);
      valuesList.forEach(value -> sqlBuilder.append(", ").append(value));
      sqlBuilder.append(")");
    }
    return sqlBuilder.toString();
  }

  /**
   * Executes the SQL query and measures the execution time.
   *
   * @param sql SQL query to execute.
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
}
