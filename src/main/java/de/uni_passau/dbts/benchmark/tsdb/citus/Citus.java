package de.uni_passau.dbts.benchmark.tsdb.citus;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.utils.Sensors;
import de.uni_passau.dbts.benchmark.utils.SqlBuilder;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Citus implements Database {
  private static final Logger LOGGER = LoggerFactory.getLogger(Citus.class);

  /** Conversion factor from bytes to gigabytes. */
  private static final int B2GB = 1024 * 1024 * 1024;

  /** SQL template for dropping a table. */
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s CASCADE;";

  /** Name of the table to use for synthetic data. */
  private static String tableName;

  /** Configuration singleton. */
  private static Config config;

  /** JDBC connection. */
  private Connection connection;

  /** Initializes an instance of the database controller. */
  public Citus() {
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
  }

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
      LOGGER.error("Initialization of Citus failed because ", e);
      throw new TsdbException(e);
    }
  }

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
      LOGGER.info("Waiting {}ms until old data has been erased.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (SQLException e) {
      LOGGER.error("An error occurred while erasing data in Citus because: {}", e.getMessage());
      LOGGER.error(e.getNextException().getMessage());

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
      LOGGER.error("Failed to close Citus connection because: {}", e.getMessage());
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
   * CREATE TABLE test (time TIMESTAMPTZ NOT NULL, bike_id VARCHAR(20) REFERENCES bikes (bike_id),
   * s_0 double precision, ..., s_12 geography, ..., s_41 double precision);
   * </code>
   *
   * <p>The table needs to be distributed in order to use it for queries. See {@link
   * #distributeTables(Statement)}
   */
  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      // Creates bikes' relational data table.
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS bikes (bike_id VARCHAR PRIMARY KEY, owner_name VARCHAR(100) NOT NULL);");
      statement.addBatch("CREATE INDEX ON bikes (bike_id);");

      // Inserts all bikes.
      statement.addBatch(getInsertBikesSql(schemaList));

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

  /**
   * Creates and executes SQL queries to create index structures on tables of metrics.
   *
   * @param statement JDBC statement object.
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

  /**
   * Distributes vanilla tables; thus, they can be stored and queried from the leaf node. See: <a
   * href="http://docs.citusdata.com/en/v8.3/develop/reference_ddl.html">Citus: Creating and
   * Modifying Distributed Tables </a>
   *
   * @param statement JDBC statement object.
   * @throws SQLException if an error occurs while distributing tables to Citus nodes.
   */
  private void distributeTables(Statement statement) throws SQLException {
    statement.execute("SELECT create_distributed_table('bikes', 'bike_id');");
    statement.execute("SELECT create_distributed_table('" + tableName + "', 'bike_id');");
    DateTime date = new DateTime(Constants.START_TIME);
    LocalDate startDate = date.toLocalDate();
    LocalDate endDate = startDate.plusDays(1);
    statement.execute(
        String.format(
            "CREATE TABLE test_2018_08_00 PARTITION OF test FOR VALUES FROM ('%s') TO ('%s');",
            startDate, endDate));
  }

  /**
   * {@inheritDoc} The batch is inserted with a single SQL query:
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., ST_SetSRID(ST_MakePoint(13.45173242391854, 48.57619798343793),4326), ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., ST_SetSRID(ST_MakePoint(13.45173242391754, 48.57619798343709),4326), ..., 65.78), ...
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
   *  SELECT time, b.bike_id, b.owner_name, s_33
   *  FROM test t, bikes b
   *  WHERE b.bike_id = t.bike_id
   *  AND b.bike_id = 'bike_51'
   *  AND time = '2018-08-30 02:00:00.0';
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  WITH location AS (
   * 	 SELECT DISTINCT ON(bike_id) bike_id, s_12, time
   * 	 FROM test t
   * 	 GROUP BY bike_id, time, s_12
   * 	 ORDER BY bike_id, time desc
   *  ) SELECT l.time, b.bike_id, b.owner_name, l.s_12
   *  FROM location l, bikes b WHERE b.bike_id = l.bike_id;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT data.bike_id, b.owner_name, data.location FROM bikes b INNER JOIN LATERAL (
   * 	 SELECT s_12 AS location, bike_id FROM test t
   * 	 WHERE t.bike_id = b.bike_id
   * 	 AND bike_id = 'bike_67'
   * 	 AND time &gt; '2018-08-30 02:00:00.0'
   * 	 AND time &lt; '2018-08-30 03:00:00.0'
   *  ) AS data ON true;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT data.second, data.bike_id, b.owner_name, data.s_12 FROM bikes b INNER JOIN LATERAL (
   * 	 SELECT date_trunc('second', time) AS second, bike_id, s_12
   * 	 FROM test t
   * 	 WHERE t.bike_id = b.bike_id
   * 	 AND t.bike_id = 'bike_51'
   * 	 AND time &gt; '2018-08-30 02:00:00.0'
   * 	 AND time &lt; '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, s_12
   * 	 HAVING AVG(s_17) &gt; 1000.000000
   *  ) AS data ON true
   *  ORDER BY data.second asc, data.bike_id;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  WITH data AS (
   * 	 SELECT date_trunc('second', time) AS second, bike_id, s_12
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_51'
   * 	 AND time &gt; '2018-08-30 02:00:00.0' AND time &lt; '2018-08-30 03:00:00.0'
   * 	 GROUP BY second, bike_id, s_12
   * 	 HAVING AVG(s_17) &gt; 1000.000000
   * 	 ORDER BY second
   *  )
   *  SELECT d.bike_id, ST_LENGTH(ST_MAKELINE(d.s_12::geometry)::geography)
   *  FROM data d
   *  GROUP BY d.bike_id;
   * </code>
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
   * Example query:
   *
   * <p><code>
   *  SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   * 	 SELECT DISTINCT(bike_id) FROM test t WHERE time &gt; '2018-08-30 03:00:00.0'
   *  );
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  WITH last_trip AS (
   * 	 SELECT date_trunc('minute', time) AS minute, bike_id FROM test t
   * 	 WHERE time &gt; '2018-08-30 02:00:00.0'
   * 	 GROUP BY minute, bike_id
   * 	 HAVING AVG(s_17) &gt; 1000.000000
   * 	 ORDER BY minute
   *  )
   *  SELECT DISTINCT ON (b.bike_id) l.minute, b.bike_id, b.owner_name
   *  FROM last_trip l, bikes b WHERE b.bike_id = l.bike_id
   *  ORDER BY b.bike_id, l.minute DESC;
   * </code>
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  WITH downsample AS (
   * 	 SELECT date_trunc('minutes', time) as minute, bike_id, AVG(s_27) AS value
   * 	 FROM test t
   * 	 WHERE bike_id = 'bike_51'
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT ST_X(s_12::geometry) AS longitude, ST_Y(s_12::geometry) AS latitude, AVG(s_34) FROM test t
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
   * {@inheritDoc} Example query:
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

  /**
   * Executes the given SQL query and measures the execution time.
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

  /**
   * Returns an SQL query, which creates a wide table for all sensors readings.
   *
   * @return SQL query.
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

  /**
   * Returns an SQL query, which inserts bikes meta data as batch.
   *
   * @param bikesList Bikes list.
   * @return SQL query.
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

  /**
   * Returns an SQL query, which inserts a batch of points into single wide table.
   *
   * <p><code>
   * INSERT INTO test VALUES ('2018-08-30 02:00:00.0', 'bike_3', 27.38, ..., ST_SetSRID(ST_MakePoint(13.45173242391854, 48.57619798343793),4326), ..., 23.43),
   * ('2018-08-30 02:00:00.02', 'bike_3', 844.44, ..., ST_SetSRID(ST_MakePoint(13.45173242391754, 48.57619798343709),4326), ..., 65.78), ...
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
