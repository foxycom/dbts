package de.uni_passau.dbts.benchmark.tsdb.timescaledb;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.utils.Sensors;
import de.uni_passau.dbts.benchmark.utils.SqlBuilder;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.ingestion.Point;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimescaleDB implements Database {

  private static final int B2GB = 1024 * 1024 * 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDB.class);
  private static final String CONVERT_TO_HYPERTABLE =
      "SELECT create_hypertable('%s', 'time', chunk_time_interval => interval '1 h');";
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s CASCADE;";

  /** Name of the wide table for synthetic data. */
  private static String tableName;

  /** Configuration singleton. */
  private static Config config;

  /** JDBC connection. */
  private Connection connection;

  /** DB size before an actual benchmark run.. */
  private long initialDbSize;

  /** Builder of SQL queries. */
  private SqlBuilder sqlBuilder;

  /** TimescaleDB table mode switch. */
  private TableMode dataModel;

  /**
   * Initializes an instance of the database controller.
   *
   * @param mode Data model to use.
   */
  public TimescaleDB(TableMode mode) {
    dataModel = mode;
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
      LOGGER.error("Initialization of TimescaleDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  /**
   * Returns the size of a given database in bytes.
   *
   * @return The size of a database in bytes.
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
    // delete old data
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      String dropBikeSql = String.format(DROP_TABLE, "bikes");
      statement.addBatch(dropBikeSql);

      if (dataModel == TableMode.NARROW_TABLE) {
        String findSensorTablesSql =
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' and "
                + "tablename LIKE '%benchmark'";
        try (ResultSet rs = statement.executeQuery(findSensorTablesSql)) {

          while (rs.next()) {
            statement.addBatch(String.format(DROP_TABLE, rs.getString("tablename")));
          }
          statement.executeBatch();
          connection.commit();
        }
      } else if (dataModel == TableMode.WIDE_TABLE) {
        statement.addBatch(String.format(DROP_TABLE, tableName));
        statement.executeBatch();
        connection.commit();
      }

      initialDbSize = getInitialSize();

      // wait for deletion complete
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
   * <p>See <a
   * href="https://docs.timescale.com/v1.0/getting-started/creating-hypertables">TimescaleDB
   * Hypertable</a>
   *
   * <p><code>
   * CREATE TABLE test (time TIMESTAMPTZ NOT NULL, bike_id VARCHAR(20) NOT NULL,
   *    s_0 DOUBLE PRECISION, s_1 DOUBLE PRECISION, ..., s_41 DOUBLE PRECISION);
   * </code>
   *
   * <p>Converts the table into a hypertable and partitions it by time:
   *
   * <p><code>
   * SELECT create_hypertable('test', 'time', chunk_time_interval =&gt; interval '1 h');
   * </code>
   */
  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      // Creates bikes' relational data table.
      String createBikesTableSql = getCreateBikesTableSql();
      statement.addBatch(createBikesTableSql);

      // Inserts all bikes.
      String insertBikesSql = getInsertBikesSql(schemaList);
      statement.addBatch(insertBikesSql);

      if (dataModel == TableMode.NARROW_TABLE) {
        for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
          String createTableSql = getCreateTableSql(sensorGroup);
          statement.addBatch(createTableSql);
        }

        statement.executeBatch();
        connection.commit();

        for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
          String convertToHyperTableSql =
              String.format(CONVERT_TO_HYPERTABLE, sensorGroup.getTableName());
          statement.execute(convertToHyperTableSql);
        }
      } else if (dataModel == TableMode.WIDE_TABLE) {
        String createTableSql = getCreateTableSql();
        statement.addBatch(createTableSql);
        statement.executeBatch();
        connection.commit();
        String convertToHyperTableSql = String.format(CONVERT_TO_HYPERTABLE, tableName);
        statement.execute(convertToHyperTableSql);
      }

      createIndexes();
    } catch (SQLException e) {
      LOGGER.error("Can't create PG table because: {}", e.getMessage());
      System.out.println(e.getNextException());
      throw new TsdbException(e);
    }
  }

  /**
   * {@inheritDoc} Reads out the size of the database with:
   *
   * <p><code>
   *   SELECT pg_database_size('test') as db_size;
   * </code> See: <a href="https://wiki.postgresql.org/wiki/Disk_Usage">PostgreSQL Disk Usage</a>
   */
  @Override
  public float getSize() throws TsdbException {
    float resultInGB = 0.0f;
    try (Statement statement = connection.createStatement()) {
      String selectSizeSql =
          String.format("SELECT pg_database_size('%s') as db_size;", config.DB_NAME);
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

  /** Creates and executes SQL queries to create index structures on tables of metrics. */
  private void createIndexes() {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      if (dataModel == TableMode.NARROW_TABLE) {
        for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
          String createIndexOnBikeSql =
              "CREATE INDEX ON " + sensorGroup.getTableName() + " (bike_id, time DESC);";
          String createIndexOnBikeAndSensorSql =
              "CREATE INDEX ON " + sensorGroup.getTableName() + " (bike_id, sensor_id, time DESC)";
          statement.addBatch(createIndexOnBikeSql);
          statement.addBatch(createIndexOnBikeAndSensorSql);

          SensorGroup gpsSensorGroup = Sensors.groupOfType("gps");
          String createGeoIndexSql =
              "CREATE INDEX ON " + gpsSensorGroup.getTableName() + " USING GIST (value)";
          statement.addBatch(createGeoIndexSql);
        }
      } else if (dataModel == TableMode.WIDE_TABLE) {
        String createIndexOnBikeSql = "CREATE INDEX ON " + tableName + " (bike_id, time DESC);";
        statement.addBatch(createIndexOnBikeSql);

        SensorGroup gpsSensorGroup = Sensors.groupOfType("gps");
        for (Sensor sensor : gpsSensorGroup.getSensors()) {
          String createGeoIndexSql =
              "CREATE INDEX ON " + tableName + " USING GIST (" + sensor.getName() + ");";
          statement.addBatch(createGeoIndexSql);
        }
      }

      statement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("Could not create PG indexes because: {}", e.getMessage());
    }
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
      if (dataModel == TableMode.NARROW_TABLE) {
        List<String> sqlQueries = getInsertOneBatchSql(batch);
        for (String query : sqlQueries) {
          statement.addBatch(query);
        }
      } else if (dataModel == TableMode.WIDE_TABLE) {
        String insertBatchSql = getInsertOneWideBatchSql(batch);
        statement.addBatch(insertBatchSql);
      }
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
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> columns = new ArrayList<>(sensor.getFields());
      columns.addAll(
          Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
      sqlBuilder =
          sqlBuilder
              .reset()
              .select(columns)
              .from(sensor.getTableName())
              .where()
              .bikes(query.getBikes())
              .and()
              .time(timestamp);
    } else if (dataModel == TableMode.WIDE_TABLE) {
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
    }
    return executeQuery(sqlBuilder.build());
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   * SELECT data.time, data.bike_id, b.owner_name, data.s_12 FROM bikes b INNER JOIN LATERAL (
   * 	SELECT * FROM test t WHERE t.bike_id = b.bike_id
   * 	ORDER BY time DESC LIMIT 1
   * ) AS data ON true;
   * </code>
   */
  @Override
  public Status lastKnownPosition(Query query) {
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> columns = new ArrayList<>(sensor.getFields());
      columns.addAll(
          Arrays.asList(
              SqlBuilder.Column.BIKE.getName(),
              SqlBuilder.Column.TIME.getName(),
              SqlBuilder.Column.SENSOR.getName()));
      sqlBuilder =
          sqlBuilder
              .reset()
              .select(columns)
              .from(sensor.getTableName())
              .where()
              .bikes(query.getBikes())
              .orderBy(SqlBuilder.Column.TIME.getName(), SqlBuilder.Order.DESC)
              .limit(1);
      sql = sqlBuilder.build();
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT data.time, data.bike_id, b.owner_name, data.%s FROM bikes b INNER JOIN LATERAL (\n"
              + "\tSELECT * FROM %s t WHERE t.bike_id = b.bike_id\n"
              + "\tORDER BY time DESC LIMIT 1\n"
              + ") AS data ON true;";
      sql = String.format(Locale.US, sql, gpsSensor.getName(), tableName, gpsSensor.getName());
    }

    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT data.bike_id, b.owner_name, data.location FROM bikes b INNER JOIN LATERAL (
   * 	SELECT s_12 AS location, bike_id FROM test t
   * 	WHERE t.bike_id = b.bike_id
   * 	AND bike_id = 'bike_8'
   * 	AND time &gt; '2018-08-30 02:00:00.0'
   * 	AND time &lt; '2018-08-30 03:00:00.0'
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
    if (dataModel == TableMode.NARROW_TABLE) {
      columns.addAll(gpsSensor.getFields());

      sqlBuilder =
          sqlBuilder
              .reset()
              .select(columns)
              .from(gpsSensor.getTableName())
              .where()
              .bikes(query.getBikes())
              .and()
              .time(query);
      sql = sqlBuilder.build();
    } else if (dataModel == TableMode.WIDE_TABLE) {
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
    }
    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT data.second, data.bike_id, b.owner_name, data.s_12 FROM bikes b INNER JOIN LATERAL (
   * 	SELECT time_bucket(interval '1 s', time) AS second, bike_id, s_12
   * 	FROM test t
   * 	WHERE t.bike_id = b.bike_id
   * 	AND t.bike_id = 'bike_7'
   * 	AND time &gt; '2018-08-30 02:00:00.0'
   * 	AND time &lt; '2018-08-30 03:00:00.0'
   * 	GROUP BY second, bike_id, s_12
   * 	HAVING AVG(s_17) &gt; 3.000000
   *  ) AS data ON true
   *  ORDER BY data.second ASC, data.bike_id;
   * </code>
   */
  @Override
  public Status identifyTrips(Query query) {
    Bike bike = query.getBikes().get(0);
    Timestamp startTimestamp = new Timestamp(Constants.START_TIMESTAMP);
    Timestamp endTimestamp = new Timestamp(Constants.START_TIMESTAMP + config.QUERY_INTERVAL);
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      sql =
          "WITH trip (second, current_value) AS (SELECT time_bucket('1 second', %s) AS second, %s(%s)"
              + " FROM %s WHERE %s = '%s' and %s > '%s' and %s < '%s' GROUP BY second HAVING %s(%s) > %f)"
              + " SELECT g.%s, t.current_value, g.%s as location FROM %s g INNER JOIN trip t "
              + "ON g.%s = t.second WHERE g.%s = '%s';";
      sql =
          String.format(
              Locale.US,
              sql,
              timeColumn,
              config.QUERY_AGGREGATE_FUN.name(),
              valueColumn,
              sensor.getTableName(),
              bikeColumn,
              bike.getName(),
              timeColumn,
              startTimestamp,
              timeColumn,
              endTimestamp,
              config.QUERY_AGGREGATE_FUN,
              valueColumn,
              query.getThreshold(),
              timeColumn,
              valueColumn,
              gpsSensor.getTableName(),
              timeColumn,
              bikeColumn,
              bike.getName());
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT data.second, data.bike_id, b.owner_name, data.s_12 FROM bikes b INNER JOIN LATERAL (\n"
              + "\tSELECT time_bucket(interval '1 s', time) AS second, bike_id, %s\n"
              + "\tFROM test t \n"
              + "\tWHERE t.bike_id = b.bike_id \n"
              + "\tAND t.bike_id = '%s'\n"
              + "\tAND time >= '%s' \n"
              + "\tAND time < '%s'\n"
              + "\tGROUP BY second, bike_id, %s\n"
              + "\tHAVING AVG(%s) >= %f\n"
              + ") AS data ON true\n"
              + "ORDER BY data.second ASC, data.bike_id;";
      sql =
          String.format(
              Locale.US,
              sql,
              gpsSensor.getName(),
              bike.getName(),
              startTimestamp,
              endTimestamp,
              gpsSensor.getName(),
              sensor.getName(),
              query.getThreshold());
    }

    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT b.bike_id, b.owner_name, ST_LENGTH(ST_MAKELINE(s_12::geometry)::geography, false)
   *  FROM bikes b INNER JOIN LATERAL (
   * 	SELECT time_bucket(interval '1 s', time) AS second, s_12 FROM test t
   * 	WHERE t.bike_id = b.bike_id AND bike_id = 'bike_8' AND time &gt; '2018-08-30 02:00:00.0'
   * 	AND time &lt; '2018-08-30 03:00:00.0'
   * 	GROUP BY second, s_12 HAVING AVG(s_17) &gt; 3.000000
   *  ) AS data ON true
   *  GROUP BY b.bike_id, b.owner_name;
   * </code>
   */
  @Override
  public Status distanceDriven(Query query) {
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    String sql = "";
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    Bike bike = query.getBikes().get(0);
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    if (dataModel == TableMode.NARROW_TABLE) {
      sql =
          "with trip_begin as (\n"
              + "\tselect time_bucket(interval '1 s', %s) as second, %s from %s where %s > %f and %s = '%s' and %s > '%s' and %s < '%s'group by second, %s order by second asc limit 1\n"
              + "), trip_end as (\n"
              + "\tselect time_bucket(interval '1 s', %s) as second, %s from %s where %s > %f and %s = '%s' and %s > '%s'and %s < '%s' group by second, %s order by second desc limit 1\n"
              + ")\n"
              + "select st_length(st_makeline(g.%s::geometry)::geography, false) from %s g, trip_begin b, trip_end e where g.%s = '%s'\n"
              + "and g.time > b.second and g.time < e.second group by g.bike_id;";
      sql =
          String.format(
              Locale.US,
              sql,
              timeColumn,
              bikeColumn,
              sensor.getTableName(),
              valueColumn,
              query.getThreshold(),
              bikeColumn,
              bike.getName(),
              timeColumn,
              startTimestamp,
              timeColumn,
              endTimestamp,
              bikeColumn,
              timeColumn,
              bikeColumn,
              sensor.getTableName(),
              valueColumn,
              query.getThreshold(),
              bikeColumn,
              bike.getName(),
              timeColumn,
              startTimestamp,
              timeColumn,
              endTimestamp,
              bikeColumn,
              valueColumn,
              sensor.getTableName(),
              bikeColumn,
              bike.getName());
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT b.bike_id, b.owner_name, ST_LENGTH(ST_MAKELINE(%s::geometry)::geography, false) \n"
              + "FROM bikes b INNER JOIN LATERAL (\n"
              + "\tSELECT time_bucket(interval '1 s', time) AS second, %s FROM %s t \n"
              + "\tWHERE t.bike_id = b.bike_id AND bike_id = '%s' AND time >= '%s' AND time <= '%s'\n"
              + "\tGROUP BY second, %s HAVING AVG(%s) > %f\n"
              + ") AS data ON true\n"
              + "GROUP BY b.bike_id, b.owner_name;";
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
              sensor.getName(),
              query.getThreshold());
    }
    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (
   * 	SELECT DISTINCT(bike_id) FROM test t WHERE time &gt; '2018-08-30 03:00:00.0'
   *  );
   * </code>
   */
  @Override
  public Status offlineBikes(Query query) {
    String sql = "";
    Sensor sensor = query.getSensor();
    Sensor gpsSensor = query.getGpsSensor();
    Bike bike = query.getBikes().get(0);

    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    if (dataModel == TableMode.NARROW_TABLE) {
      sql =
          "with trip as (\n"
              + "\tselect time_bucket(interval '1 s', %s) as second, %s, avg(%s) as value from %s \n"
              + "\twhere %s > %f and %s = '%s' \n"
              + "\tgroup by second, %s\n"
              + ") \n"
              + "select time_bucket(interval '%d ms', g.%s) as half_minute, avg(t.%s), t.%s, st_makeline(g.%s::geometry) from %s g, trip t \n"
              + "where g.%s = '%s' and g.%s = t.second and g.%s > '%s' and g.%s < '%s'\n"
              + "group by half_minute, t.%s;";
      sql =
          String.format(
              Locale.US,
              sql,
              timeColumn,
              bikeColumn,
              valueColumn,
              sensor.getTableName(),
              valueColumn,
              config.QUERY_LOWER_LIMIT,
              bikeColumn,
              bike.getName(),
              bikeColumn,
              config.TIME_BUCKET,
              timeColumn,
              valueColumn,
              bikeColumn,
              valueColumn,
              gpsSensor.getTableName(),
              bikeColumn,
              bike.getName(),
              timeColumn,
              timeColumn,
              startTimestamp,
              timeColumn,
              endTimestamp,
              bikeColumn);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT DISTINCT(bike_id) FROM bikes WHERE bike_id NOT IN (\n"
              + "\tSELECT DISTINCT(bike_id) FROM %s t WHERE time > '%s'\n"
              + ");";
      sql = String.format(Locale.US, sql, tableName, endTimestamp);
    }
    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT data.minute, data.bike_id, b.owner_name FROM bikes b INNER JOIN LATERAL (
   * 	SELECT time_bucket(interval '1 min', time) AS minute, bike_id FROM test t
   * 	WHERE t.bike_id = b.bike_id AND time &gt; '2018-08-29 18:00:00.0'
   * 	AND s_17 IS NOT NULL
   * 	GROUP BY minute, bike_id
   * 	HAVING AVG(s_17) &gt; 3.000000
   * 	ORDER BY minute DESC LIMIT 1
   *  ) AS data
   *  ON true
   *  ORDER BY b.bike_id, data.minute DESC;
   * </code>
   */
  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Sensor sensor = query.getSensor();
    List<String> columns =
        new ArrayList<>(
            Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      columns.addAll(sensor.getFields());
      sqlBuilder =
          sqlBuilder
              .reset()
              .select(columns)
              .from(sensor.getTableName())
              .where()
              .value(SqlBuilder.Op.GREATER, query.getThreshold())
              .and()
              .bikes(query.getBikes())
              .and()
              .time(query);
      sql = sqlBuilder.build();
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Timestamp timestamp = new Timestamp(query.getEndTimestamp());
      sql =
          "SELECT data.minute, data.bike_id, b.owner_name FROM bikes b INNER JOIN LATERAL (\n"
              + "\tSELECT time_bucket(interval '1 min', time) AS minute, bike_id FROM test t \n"
              + "\tWHERE t.bike_id = b.bike_id AND time > '%s'\n"
              + "\tAND %s IS NOT NULL\n"
              + "\tGROUP BY minute, bike_id\n"
              + "\tHAVING AVG(%s) > %f\n"
              + "\tORDER BY minute DESC LIMIT 1\n"
              + ") AS data \n"
              + "ON true \n"
              + "ORDER BY b.bike_id, data.minute DESC;";
      sql =
          String.format(
              Locale.US, sql, timestamp, sensor.getName(), sensor.getName(), query.getThreshold());
    }
    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT data.minute, b.bike_id, b.owner_name, data.value FROM bikes b INNER JOIN LATERAL (
   * 	SELECT time_bucket(interval '1 min', time) AS minute, AVG(s_27) AS value
   * 	FROM test t WHERE t.bike_id = b.bike_id
   * 	AND bike_id = 'bike_7'
   * 	AND time &gt; '2018-08-30 02:00:00.0'
   * 	AND time &lt; '2018-08-30 03:00:00.0'
   * 	GROUP BY minute
   *  ) AS data ON true
   *  ORDER BY data.minute ASC, b.bike_id;
   * </code>
   */
  @Override
  public Status downsample(Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    Timestamp startTimestamp = new Timestamp(query.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(query.getEndTimestamp());
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> aggregatedColumns = new ArrayList<>(sensor.getFields());
      List<String> plainColumns =
          new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));
      sqlBuilder =
          sqlBuilder
              .reset()
              .select(aggregatedColumns, plainColumns, query.getAggrFunc(), config.TIME_BUCKET)
              .from(sensor.getTableName())
              .where()
              .time(query)
              .groupBy(
                  Arrays.asList(Constants.TIME_BUCKET_ALIAS, SqlBuilder.Column.BIKE.getName()));
      sql = sqlBuilder.build();
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT data.minute, b.bike_id, b.owner_name, data.value FROM bikes b INNER JOIN LATERAL (\n"
              + "\tSELECT time_bucket(interval '1 min', time) AS minute, AVG(%s) AS value\n"
              + "\tFROM test t WHERE t.bike_id = b.bike_id\n"
              + "\tAND bike_id = '%s'\n"
              + "\tAND time >= '%s'\n"
              + "\tAND time <= '%s'\n"
              + "\tGROUP BY minute\n"
              + ") AS data ON true\n"
              + "ORDER BY data.minute ASC, b.bike_id;";
      sql =
          String.format(
              Locale.US, sql, sensor.getName(), bike.getName(), startTimestamp, endTimestamp);
    }

    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example usage:
   *
   * <p><code>
   *  SELECT ST_X(s_12::geometry) AS longitude,
   *  ST_Y(s_12::geometry) AS latitude, AVG(s_34) FROM test t
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
    GeoPoint startPoint = Constants.GRID_START_POINT;
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      sql =
          "with map as (select (st_dump(map.geom)).geom from (\n"
              + "\tselect st_setsrid(st_collect(grid.geom),4326) as geom from ST_CreateGrid(40, 90, 0.0006670, 0.0006670,"
              + " %f, %f) as grid\n"
              + ") map)\n"
              + "select m.geom as cell, avg(a.%s) from %s g, map m, (\n"
              + "\tselect time_bucket(interval '1 sec', ab.%s) as second, avg(%s) as value, %s from %s ab "
              + "where ab.%s > '%s' and ab.%s < '%s' group by second, %s\n"
              + ") a\n"
              + "where g.%s = a.%s and a.second = g.time and st_contains(m.geom, g.value::geometry) group by m.geom;";
      sql =
          String.format(
              Locale.US,
              sql,
              startPoint.getLongitude(),
              startPoint.getLatitude(),
              valueColumn,
              gpsSensor.getTableName(),
              timeColumn,
              valueColumn,
              bikeColumn,
              sensor.getTableName(),
              timeColumn,
              startTimestamp,
              timeColumn,
              endTimestamp,
              bikeColumn,
              bikeColumn,
              bikeColumn);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT ST_X(%s::geometry) AS longitude, \n"
              + "ST_Y(%s::geometry) AS latitude, AVG(%s) FROM %s t\n"
              + "WHERE time >= '%s' AND time <= '%s'\n"
              + "GROUP BY longitude, latitude;";
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
    }
    return executeQuery(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *  SELECT b.bike_id, b.owner_name, data.location FROM bikes b INNER JOIN LATERAL (
   * 	SELECT s_12 AS location FROM test t WHERE t.bike_id = b.bike_id
   * 	ORDER BY time DESC LIMIT 1
   *  ) AS data ON true
   *  WHERE ST_CONTAINS(
   * 	ST_BUFFER(ST_SETSRID(ST_MAKEPOINT(13.431947, 48.566736),4326)::geography, 500
   * 			 )::geometry, data.location::geometry);
   * </code>
   */
  @Override
  public Status bikesInLocation(Query query) {
    String sql = "";
    Sensor gpsSensor = query.getGpsSensor();
    if (dataModel == TableMode.NARROW_TABLE) {
      sql =
          "with last_location as (\n"
              + "\tselect %s, last(%s, %s) as location from %s group by %s\n"
              + ") select * from last_location l where st_contains(st_buffer(st_setsrid(st_makepoint(%f, %f),4326)::geography, %d)::geometry, l.location::geometry);";
      sql =
          String.format(
              Locale.US,
              sql,
              SqlBuilder.Column.BIKE.getName(),
              SqlBuilder.Column.VALUE.getName(),
              SqlBuilder.Column.TIME.getName(),
              gpsSensor.getTableName(),
              SqlBuilder.Column.BIKE.getName(),
              Constants.SPAWN_POINT.getLongitude(),
              Constants.SPAWN_POINT.getLatitude(),
              config.RADIUS);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      sql =
          "SELECT b.bike_id, b.owner_name, data.location FROM bikes b INNER JOIN LATERAL (\n"
              + "\tSELECT %s AS location FROM %s t WHERE t.bike_id = b.bike_id\n"
              + "\tORDER BY time DESC LIMIT 1\n"
              + ") AS data ON true\n"
              + "WHERE ST_CONTAINS(\n"
              + "\tST_BUFFER(ST_SETSRID(ST_MAKEPOINT(%f, %f),4326)::geography, %d\n"
              + "\t\t\t )::geometry, data.location::geometry);";
      sql =
          String.format(
              Locale.US,
              sql,
              gpsSensor.getName(),
              tableName,
              Constants.SPAWN_POINT.getLongitude(),
              Constants.SPAWN_POINT.getLatitude(),
              config.RADIUS);
    }
    return executeQuery(sql);
  }

  /**
   * Executes the SQL query and measures the execution time.
   *
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
   * Returns an SQL statement, which creates a narrow table for a single sensor group.
   *
   * @param sensorGroup Sensor group to create a table for.
   * @return SQL query.
   */
  private String getCreateTableSql(SensorGroup sensorGroup) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder
        .append("CREATE TABLE ")
        .append(sensorGroup.getTableName())
        .append(
            " (time TIMESTAMPTZ NOT NULL, "
                + "bike_id VARCHAR(20) REFERENCES bikes (bike_id), sensor_id VARCHAR(20) NOT NULL");

    sensorGroup.getFields().stream()
        .forEach(
            field ->
                sqlBuilder
                    .append(", ")
                    .append(field)
                    .append(" ")
                    .append(sensorGroup.getDataType())
                    .append(" NULL"));
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  /**
   * Returns an SQL statement, which creates a wide table for all sensor readings.
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
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  /**
   * Returns an SQL query for creating the bikes meta table.
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
   * Returns an SQL batch insert query for insert bikes meta data.
   *
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
   * Creates a list of SQL batch queries. Each string writes points to a distinct narrow table.
   *
   * @param batch Batch of data points.
   * @return List of sensor group specific SQL queries.
   */
  private List<String> getInsertOneBatchSql(Batch batch) {
    Map<Sensor, Point[]> entries = batch.getEntries();
    Bike bike = batch.getBike();
    StringBuilder sqlBuilder = new StringBuilder();

    List<String> sensorQueries = new ArrayList<>(bike.getSensors().size());
    for (Sensor sensor : bike.getSensors()) {
      if (entries.get(sensor).length == 0) {
        continue;
      }

      sqlBuilder
          .append("INSERT INTO ")
          .append(sensor.getSensorGroup().getTableName())
          .append(" VALUES ");

      boolean firstIteration = true;
      for (Point point : entries.get(sensor)) {
        if (firstIteration) {
          firstIteration = false;
        } else {
          sqlBuilder.append(", ");
        }
        Timestamp timestamp = new Timestamp(point.getTimestamp());
        sqlBuilder
            .append("('")
            .append(timestamp)
            .append("', ")
            .append("'")
            .append(bike.getName())
            .append("', ")
            .append("'")
            .append(sensor.getName())
            .append("'");
        if (sensor.getFields().size() == 1) {
          sqlBuilder.append(", ").append(point.getValue());
        } else {
          String[] values = point.getValues();
          Arrays.stream(values).forEach(value -> sqlBuilder.append(", ").append(value));
        }
        sqlBuilder.append(")");
      }
      sqlBuilder.append(";");

      sensorQueries.add(sqlBuilder.toString());
      sqlBuilder.setLength(0);
    }

    return sensorQueries;
  }

  /**
   * Creates an SQL query, which inserts a batch of points into a single wide table.
   *
   * @param batch Batch of points.
   * @return SQL query.
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

  /** Determines which logical data model to use when storing data points. */
  public enum TableMode {
    NARROW_TABLE,
    WIDE_TABLE
  }
}
