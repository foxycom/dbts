package de.uni_passau.dbts.benchmark.tsdb.vertica;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.GpsSensor;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

/**
 * Implementation of benchmark scenarios for Vertica.
 */
public class Vertica implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(Vertica.class);

  /** SQL template for dropping a table. */
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s CASCADE;";

  /** Configuration singleton. */
  private Config config;

  /** JDBC connection. */
  private Connection connection;

  /** Name of the table to use for synthetic data. */
  private String tableName = "test";

  /** File containing queries templates. */
  private STGroupFile templatesFile;

  /**
   * Creates an instance of the Vertica controller.
   */
  public Vertica() {
    config = ConfigParser.INSTANCE.config();
    templatesFile = new STGroupFile("templates/vertica/queries.stg");
  }

  @Override
  public void init() throws TsdbException {
    try {
      connection =
          DriverManager.getConnection(
              String.format(Constants.VERTICA_URL, config.HOST, config.PORT, config.DB_NAME),
              Constants.VERTICA_USER,
              Constants.VERTICA_PASSWD);
    } catch (SQLException e) {
      LOGGER.error("Initialization of Vertica failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      String dropBikeSql = String.format(DROP_TABLE, "bikes");
      statement.addBatch(dropBikeSql);

      statement.addBatch(String.format(DROP_TABLE, tableName));
      statement.executeBatch();
      connection.commit();

      LOGGER.info("Waiting {}ms until old data has been erased.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (SQLException e) {
      LOGGER.warn("An error occurred while erasing Vertica data because: {}", e.getMessage());
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
      LOGGER.error("Failed to close Vertica connection because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Maps the data schema concepts:
   *
   * <ul>
   *   <li>Bike name -&gt; A column in the table
   *   <li>Sensor names -&gt; Columns in the table
   *   <li>Sensor values -&gt; Rows in the table
   * </ul>
   *
   * <p><code>
   * CREATE TABLE test (time TIMESTAMPTZ NOT NULL, bike_id VARCHAR REFERENCES bikes (bike_id),
   * s_0 DOUBLE PRECISION, ..., s_12 GEOGRAPHY, ...);
   * </code>
   */
  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      // Creates bikes' relational data table.
      statement.execute(getCreateBikesTableSql());

      // Inserts all bikes.
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO bikes (bike_id, owner_name) VALUES (?, ?)");
      for (Bike bike : schemaList) {
        preparedStatement.setString(1, bike.getName());
        preparedStatement.setString(2, bike.getOwnerName());
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();

      statement.execute(getCreateTableSql());
      connection.commit();

      // Temporal hard-coded definitions of projections.
      ScriptRunner sr = new ScriptRunner(connection);
      Reader reader = new BufferedReader(new FileReader("vertica/design.sql"));
      sr.runScript(reader);
    } catch (SQLException e) {
      LOGGER.error("Could not create Vertica table because: {}", e.getMessage());
      if (e.getNextException() != null) {
        System.out.println(e.getNextException().getMessage());
      }
      throw new TsdbException(e);
    } catch (FileNotFoundException e) {
      LOGGER.error("Could not register projections because: {}", e.getMessage());
      throw new TsdbException(e);
    }
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  /**
   * {@inheritDoc} The batch is inserted through a prepared SQL statement.
   * See {@link #getPreparedSql(Batch)}.
   */
  @Override
  public Status insertOneBatch(Batch batch) {
    String preparedSql = getPreparedSql(batch);
    Bike bike = batch.getBike();
    List<Sensor> sensors = bike.getSensors();

    long startTime;
    long endTime;
    try (PreparedStatement statement = connection.prepareStatement(preparedSql)) {
      connection.setAutoCommit(false);

      Map<Long, List<String>> b = batch.transform();
      for (Long timestamp : b.keySet()) {
        statement.setTimestamp(1, new Timestamp(timestamp));
        statement.setString(2, bike.getName());
        List<String> values = b.get(timestamp);
        for (int i = 0; i < sensors.size(); i++) {
          int pos = i + 3;
          Sensor sensor = sensors.get(i);
          if (sensor instanceof GpsSensor) {
            statement.setString(pos, values.get(i));
          } else {
            String value = values.get(i);
            if (value.equals("NULL")) {
              statement.setNull(pos, Types.INTEGER);
            } else {
              statement.setFloat(pos, Float.parseFloat(values.get(i)));
            }
          }
        }
        statement.addBatch();
      }

      int[] res = null;
      startTime = System.nanoTime();
      try {
        statement.executeBatch();
      } catch (BatchUpdateException e) {
        LOGGER.debug("Batch update exception ", e);
        res = e.getUpdateCounts();
        LOGGER.debug("Rows: {}", res);
      }
      connection.commit();
      endTime = System.nanoTime();
      return new Status(true, endTime - startTime);
    } catch (SQLException e) {
      LOGGER.debug("Could not insert batch because ", e);
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT time, bike_id, s_33 FROM test WHERE bike_id = 'bike_2' AND time = '2018-08-30 02:00:00.0';
   * </code>
   */
  @Override
  public Status precisePoint(Query query) {
    Timestamp timestamp = new Timestamp(query.getStartTimestamp());
    Bike bike = query.getBikes().get(0);
    Sensor sensor = query.getSensor();
    StringBuilder sqlBuilder = new StringBuilder("SELECT time, bike_id, ");
    sqlBuilder
        .append(sensor.getName())
        .append(" FROM ")
        .append(tableName)
        .append(" WHERE bike_id = '")
        .append(bike.getName())
        .append("' AND time = '")
        .append(timestamp)
        .append("';");
    return executeQuery(sqlBuilder.toString());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT s_12 AS location, t.bike_id, b.owner_name
   * FROM test t, bikes b
   * WHERE b.bike_id = t.bike_id AND t.bike_id = 'bike_32'
   * AND time &gt; '2018-08-30 02:00:00.0'
   * AND time &lt; '2018-08-30 03:00:00.0';
   * </code>
   */
  @Override
  public Status gpsPathScan(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    Bike bike = query.getBikes().get(0);
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    ST template = templatesFile.getInstanceOf("gpsPathScan");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("bike", bike.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * WITH data AS (
   *      SELECT date_trunc('second', time) AS SECOND,
   *      bike_id, s_12
   *  	FROM test t
   *  	WHERE bike_id = 'bike_2'
   *  	AND time &gt; '2018-08-30 02:00:00.0'
   *  	AND time &lt; '2018-08-30 03:00:00.0'
   *  	GROUP BY second, bike_id, s_12
   *  	HAVING AVG(s_17) &gt; 1000.0
   *  )
   *  SELECT d.second, d.bike_id, b.owner_name, d.s_12 FROM bikes b, data d
   *  WHERE d.bike_id = b.bike_id
   *  ORDER BY d.second ASC, d.bike_id;
   * </code>
   */
  @Override
  public Status identifyTrips(Query query) {
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    Bike bike = query.getBikes().get(0);
    ST template = templatesFile.getInstanceOf("identifyTrips");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("sensor", sensor.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp)
        .add("bike", bike.getName())
        .add("threshold", query.getThreshold());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   *     SELECT DISTINCT(bike_id) FROM test t WHERE time &gt; '2018-08-30 03:00:00.0'
   * );
   * </code>
   */
  @Override
  public Status offlineBikes(Query query) {
    Timestamp timestamp = new Timestamp(query.getEndTimestamp());
    ST template = templatesFile.getInstanceOf("offlineBikes");
    template.add("tableName", tableName).add("time", timestamp);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT DISTINCT(t.bike_id), b.owner_name,
   *     LAST_VALUE(minute) OVER (PARTITION BY t.bike_id ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
   *     FROM (
   *         SELECT date_trunc('minute', time) AS minute, bike_id
   *         FROM test
   *         GROUP BY minute, bike_id
   *         HAVING AVG(s_17) &gt; 1000.0
   *     ) t, bikes b
   *     WHERE t.bike_id = b.bike_id;
   * </code>
   */
  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();
    ST template = templatesFile.getInstanceOf("lastTimeActivelyDriven");
    template
        .add("tableName", tableName)
        .add("sensor", sensor.getName())
        .add("threshold", query.getThreshold());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * WITH downsample AS (
   * 	 SELECT date_trunc('minute', time) AS minute, bike_id, AVG(s_27) AS value
   *   FROM test t
   *   WHERE bike_id = 'bike_2'
   *   AND time &gt; '2018-08-30 02:00:00.0'
   *   AND time &lt; '2018-08-30 03:00:00.0'
   *   GROUP BY bike_id, minute
   * ) SELECT d.minute, b.bike_id, b.owner_name, d.value
   * FROM downsample d, bikes b WHERE b.bike_id = d.bike_id
   * ORDER BY d.minute, b.bike_id;
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
        .add("bike", bike.getName())
        .add("sensor", sensor.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp);
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT DISTINCT(t.bike_id), b.owner_name,
   * LAST_VALUE(t.s_12) OVER(PARTITION BY t.bike_id ORDER BY (time) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as location,
   * LAST_VALUE(time) OVER(PARTITION BY t.bike_id ORDER BY (time) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as time
   * FROM test t, bikes b
   * WHERE t.bike_id = b.bike_id;
   * </code>
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("lastKnownPosition");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT ST_X(s_12) as longitude, ST_Y(s_12) as latitude, AVG(s_34)
   *     FROM test t
   *     WHERE time &gt; '2018-08-30 02:00:00.0' AND time &lt; '2018-08-30 03:00:00.0'
   *     GROUP BY longitude, latitude;
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
        .add("start", startTimestamp)
        .add("end", endTimestamp)
        .add("gpsSensor", gpsSensor.getName())
        .add("sensor", sensor.getName());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * WITH data AS (
   *     SELECT date_trunc('second', time) AS second, bike_id, ST_X(s_12) AS longitude, ST_Y(s_12) AS latitude
   *     FROM test t
   *     WHERE bike_id = 'bike_0'
   *     AND time &gt; '2018-08-30 02:00:00.0' and time &lt; '2018-08-30 03:00:00.0'
   *     GROUP BY second, bike_id, longitude, latitude
   *     HAVING AVG(s_17) &gt; 1000.0
   *     ORDER BY second
   * )
   * SELECT d.bike_id, b.owner_name,
   * ST_Length(ST_GeographyFromText(CONCAT(CONCAT(
   *     'LINESTRING(', LISTAGG(longitude|| ' ' ||latitude USING PARAMETERS max_length=10000000)
   * ), ')'))) FROM data d, bikes b
   * WHERE d.bike_id = b.bike_id
   * GROUP BY d.bike_id, b.owner_name;
   * </code>
   */
  @Override
  public Status distanceDriven(Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    Sensor gpsSensor = query.getGpsSensor();
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    ST template = templatesFile.getInstanceOf("distanceDriven");
    template
        .add("tableName", tableName)
        .add("gpsSensor", gpsSensor.getName())
        .add("sensor", sensor.getName())
        .add("start", startTimestamp)
        .add("end", endTimestamp)
        .add("bike", bike.getName())
        .add("threshold", query.getThreshold());
    String debig = template.render();
    return executeQuery(debig);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT b.bike_id, b.owner_name, pos.pos FROM bikes b,
   * (SELECT DISTINCT(bike_id),
   *     LAST_VALUE(s_12) OVER (PARTITION BY bike_id ORDER BY (time)
   *         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS pos FROM test) AS pos
   * WHERE b.bike_id = pos.bike_id
   * AND ST_Contains(ST_GeographyFromText('POLYGON((13.4406567 48.5723195,
   *   13.4373522 48.5707861, 13.4373522 48.5662708,
   *   13.4443045 48.5645384, 13.4489393 48.5683155,
   *   13.4492826 48.5710701, 13.4406567 48.5723195))'), pos.pos);
   * </code>
   */
  @Override
  public Status bikesInLocation(Query query) {
    Sensor gpsSensor = query.getGpsSensor();
    ST template = templatesFile.getInstanceOf("bikesInLocation");
    template.add("tableName", tableName).add("gpsSensor", gpsSensor.getName());
    String debug = template.render();
    return executeQuery(debug);
  }

  /**
   * Returns an SQL query that creates a table with bikes' meta data.
   *
   * @return SQL query.
   */
  private String getCreateBikesTableSql() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(
        "CREATE TABLE IF NOT EXISTS bikes (bike_id VARCHAR PRIMARY KEY, owner_name VARCHAR(100) NOT NULL);");
    return sqlBuilder.toString();
  }

  /**
   * Returns an SQL query that creates a wide table for all sensors readings.
   *
   * <p><code>
   *   CREATE TABLE test (time TIMESTAMPTZ NOT NULL, bike_id VARCHAR REFERENCES bikes (bike_id),
   *   s_0 DOUBLE PRECISION, ..., s_12 GEOGRAPHY, ...);
   * </code></p>
   *
   * @return SQL query.
   */
  private String getCreateTableSql() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(tableName)
        .append(" (time TIMESTAMPTZ NOT NULL, ")
        .append("bike_id VARCHAR REFERENCES bikes (bike_id)");
    config.SENSORS.forEach(
        sensor ->
            sqlBuilder
                .append(", ")
                .append(sensor.getName())
                .append(" ")
                .append(sensor.getDataType()));
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  /**
   * Returns a prepared SQL statement that is used to insert sensor readings into the wide table.
   *
   * @param batch Batch of points.
   * @return Prepared SQL statement.
   */
  private String getPreparedSql(Batch batch) {
    Bike bike = batch.getBike();
    List<Sensor> sensors = bike.getSensors();
    StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES (?, ?");
    sensors.forEach(
        sensor -> {
          if (sensor instanceof GpsSensor) {
            sb.append(", ST_GeographyFromText(?)");
          } else {
            sb.append(", ?");
          }
        });
    sb.append(")");
    return sb.toString();
  }

  /**
   * Executes the given SQL query and measures elapsed time.
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
}
