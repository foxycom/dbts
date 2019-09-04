package cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb;

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
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.sql.*;
import java.util.*;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GeoPoint;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimescaleDB implements IDatabase {

  private static final int B2GB = 1024 * 1024 * 1024;
  private Connection connection;
  private static String tableName;
  private static Config config;
  private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDB.class);
  private static final String CONVERT_TO_HYPERTABLE =
      "SELECT create_hypertable('%s', 'time', chunk_time_interval => interval '1 h');";
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s CASCADE;";
  private long initialDbSize;
  private SqlBuilder sqlBuilder;
  private TableMode dataModel;

  public enum  TableMode {
    NARROW_TABLE,
    WIDE_TABLE
  }

  /**
   * Initializes an instance of the database controller.
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
      connection = DriverManager.getConnection(
          String.format(Constants.POSTGRESQL_URL, config.HOST, config.PORT, config.DB_NAME),
          Constants.POSTGRESQL_USER,
          Constants.POSTGRESQL_PASSWD
      );
    } catch (Exception e) {
      LOGGER.error("Initialize TimescaleDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  /*
   * Returns the size of a given database in bytes.
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
    //delete old data
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      String deleteBikesSql = String.format(DROP_TABLE, "bikes");
      statement.addBatch(deleteBikesSql);

      if (dataModel == TableMode.NARROW_TABLE) {
        String findSensorTablesSql = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' and " +
                "tablename LIKE '%benchmark'";
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
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
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
          String convertToHyperTableSql = String.format(CONVERT_TO_HYPERTABLE, sensorGroup.getTableName());
          statement.execute(convertToHyperTableSql);
        }
      } else if (dataModel == TableMode.WIDE_TABLE) {
        String createTableSql = getCreateTableSql();
        statement.addBatch(createTableSql);
        statement.executeBatch();
        connection.commit();
        String converToHyperTableSql = String.format(CONVERT_TO_HYPERTABLE, tableName);
        statement.execute(converToHyperTableSql);
      }

      createIndexes();
      createGridFunction();
    } catch (SQLException e) {
      LOGGER.error("Can't create PG table because: {}", e.getMessage());
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
      String selectSizeSql = String.format("SELECT pg_database_size('%s') as db_size;", config.DB_NAME);
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

  /*
   * Creates and executes SQL queries to create index structures on tables of metrics.
   */
  private void createIndexes() {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      if (dataModel == TableMode.NARROW_TABLE) {
        for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
          String createIndexOnBikeSql = "CREATE INDEX ON " + sensorGroup.getTableName() + " (bike_id, time DESC);";
          String createIndexOnBikeAndSensorSql = "CREATE INDEX ON " + sensorGroup.getTableName() + " (bike_id, sensor_id, time DESC)";
          statement.addBatch(createIndexOnBikeSql);
          statement.addBatch(createIndexOnBikeAndSensorSql);

          SensorGroup gpsSensorGroup = Sensors.groupOfType("gps");
          String createGeoIndexSql = "CREATE INDEX ON " + gpsSensorGroup.getTableName() + " USING GIST (value)";
          statement.addBatch(createGeoIndexSql);
        }
      } else if (dataModel == TableMode.WIDE_TABLE) {
        String createIndexOnBikeSql = "CREATE INDEX ON " + tableName + " (bike_id, time DESC);";
        statement.addBatch(createIndexOnBikeSql);

        SensorGroup gpsSensorGroup = Sensors.groupOfType("gps");
        for (Sensor sensor : gpsSensorGroup.getSensors()) {
          String createGeoIndexSql = "CREATE INDEX ON " + tableName + " USING GIST (" + sensor.getName() + ")";
          statement.addBatch(createGeoIndexSql);
        }
      }

      statement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("Could not create PG indexes because: {}", e.getMessage());
    }
  }

  /*
   * Registers a function in SQL, which generates a grid for a heat map.
   */
  private void createGridFunction() {
    try (Statement statement = connection.createStatement()) {
      String sql = "CREATE OR REPLACE FUNCTION ST_CreateGrid(\n" +
              "        nrow integer, ncol integer,\n" +
              "        xsize float8, ysize float8,\n" +
              "        x0 float8 DEFAULT 0, y0 float8 DEFAULT 0,\n" +
              "        OUT \"row\" integer, OUT col integer,\n" +
              "        OUT geom geometry)\n" +
              "    RETURNS SETOF record AS\n" +
              "$$\n" +
              "SELECT i + 1 AS row, j + 1 AS col, ST_Translate(cell, j * $3 + $5, i * $4 + $6) AS geom\n" +
              "FROM generate_series(0, $1 - 1) AS i,\n" +
              "     generate_series(0, $2 - 1) AS j,\n" +
              "(\n" +
              "SELECT ('POLYGON((0 0, 0 '||$4||', '||$3||' '||$4||', '||$3||' 0,0 0))')::geometry AS cell\n" +
              ") AS foo;\n" +
              "$$ LANGUAGE sql IMMUTABLE STRICT;";
      statement.execute(sql);
    } catch (SQLException e) {
      LOGGER.error("Could not register grid function.");
    }
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
    long st;
    long en;
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
      st = System.nanoTime();
      statement.executeBatch();
      connection.commit();
      en = System.nanoTime();
      return new Status(true, en - st);
    } catch (SQLException e) {
      System.out.println(e.getNextException().getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * Selects data points, which are stored with a given timestamp.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT value, time, bike_id FROM emg_benchmark WHERE (bike_id = 'bike_10') AND (time = '2018-08-29 18:00:00.0');
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_8') AND (time = '2018-08-29 18:00:00.0');
   * </code></p>
   *
   * @param preciseQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    SensorGroup sensorGroup = preciseQuery.getSensorGroup();
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> columns = new ArrayList<>(sensorGroup.getFields());
      columns.addAll(Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
      sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName())
              .where().bikes(preciseQuery.getDeviceSchemas()).and().time(preciseQuery);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      List<String> columns = Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName(),
              sensor.getName());
      sqlBuilder = sqlBuilder.reset().select(columns).from(tableName).where().bikes(preciseQuery.getDeviceSchemas())
              .and().time(preciseQuery);
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Executes a scan of values over a given period of time on a given number of bikes.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT value FROM emg_benchmark WHERE (bike_id = 'bike_8')
   *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_8')
   *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0') AND s_40 IS NOT NULL;
   * </code></p>
   *
   * @param rangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<String> columns = new ArrayList<>();
    if (dataModel == TableMode.NARROW_TABLE) {
      columns.add(SqlBuilder.Column.VALUE.getName());

      sqlBuilder = sqlBuilder.reset().select(columns).from(rangeQuery.getSensorGroup().getTableName())
              .where().bikes(rangeQuery.getDeviceSchemas()).and().time(rangeQuery);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = rangeQuery.getSensorGroup().getSensors().get(0);
      columns.addAll(Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName(), sensor.getName()));
      sqlBuilder = sqlBuilder.reset().select(columns).from(tableName).where().bikes(rangeQuery.getDeviceSchemas())
              .and().time(rangeQuery).and().isNotNull(sensor.getName());
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
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
   *  SELECT time, bike_id, s_12 FROM test WHERE (bike_id = 'bike_10') AND (time >= '2018-08-29 18:00:00.0'
   *  AND time <= '2018-08-29 19:00:00.0') AND s_12 IS NOT NULL;
   * </code></p>
   *
   * @param rangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status gpsRangeQuery(RangeQuery rangeQuery) {
    SensorGroup sensorGroup = rangeQuery.getSensorGroup();
    List<String> columns = new ArrayList<>(Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
    if (dataModel == TableMode.NARROW_TABLE) {
      columns.addAll(sensorGroup.getFields());

      sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName())
              .where().bikes(rangeQuery.getDeviceSchemas()).and().time(rangeQuery);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor gpsSensor = rangeQuery.getSensorGroup().getSensors().get(0);
      columns.add(gpsSensor.getName());
      sqlBuilder = sqlBuilder.reset().select(columns).from(tableName).where().bikes(rangeQuery.getDeviceSchemas())
              .and().time(rangeQuery).and().isNotNull(gpsSensor.getName());
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
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
   *  SELECT time_bucket(interval '1 s', time) AS second, AVG(s_40), s_12 FROM test WHERE bike_id = 'bike_10'
   *  AND s_40 > 3.000000 AND s_12 IS NOT NULL GROUP BY second, s_12;
   * </code></p>
   *
   * @param rangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status gpsValueRangeQuery(GpsValueRangeQuery rangeQuery) {
    DeviceSchema deviceSchema = rangeQuery.getDeviceSchemas().get(0);
    Timestamp startTimestamp = new Timestamp(Constants.START_TIMESTAMP);
    Timestamp endTimestamp = new Timestamp(Constants.START_TIMESTAMP + config.QUERY_INTERVAL);
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    SensorGroup plainSensorGroup = rangeQuery.getSensorGroup();
    SensorGroup gpsSensorGroup = rangeQuery.getGpsSensorGroup();
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      sql = "WITH trip (second, current_value) AS (SELECT time_bucket('1 second', %s) AS second, %s(%s)"
              + " FROM %s WHERE %s = '%s' and %s > '%s' and %s < '%s' GROUP BY second HAVING %s(%s) > %f)"
              + " SELECT g.%s, t.current_value, g.%s as location FROM %s g INNER JOIN trip t "
              + "ON g.%s = t.second WHERE g.%s = '%s';";
      sql = String.format(Locale.US, sql, timeColumn, config.QUERY_AGGREGATE_FUN.name(), valueColumn,
              plainSensorGroup.getTableName(), bikeColumn, deviceSchema.getDevice(), timeColumn, startTimestamp,
              timeColumn, endTimestamp, config.QUERY_AGGREGATE_FUN, valueColumn, rangeQuery.getValueThreshold(),
              timeColumn, valueColumn, gpsSensorGroup.getTableName(), timeColumn, bikeColumn, deviceSchema.getDevice());
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = rangeQuery.getSensorGroup().getSensors().get(0);
      Sensor gpsSensor = rangeQuery.getGpsSensorGroup().getSensors().get(0);
      sql = "SELECT time_bucket(interval '1 s', %s) AS second, AVG(%s), %s FROM %s WHERE %s = '%s' AND %s > %f AND %s IS NOT NULL GROUP BY second, %s;";
      sql = String.format(Locale.US, sql, timeColumn, sensor.getName(), gpsSensor.getName(), tableName,
              bikeColumn, deviceSchema.getDevice(), sensor.getName(),
              rangeQuery.getValueThreshold(), gpsSensor.getName(), gpsSensor.getName());
    }
    return executeQueryAndGetStatus(sql);
  }

  /**
   * Computes an average value per distance which has been driven within a given time range, e. g. energy consumption.
   * A trip is identified by a sensor value which exceeds certain threshold.
   *
   * NARROW_TABLE:
   * <p><code>
   *  WITH trip AS (
   * 	SELECT time_bucket(interval '1 s', time) AS second, bike_id, AVG(value) AS value FROM emg_benchmark
   * 	WHERE value > 3.000000 AND bike_id = 'bike_10'
   * 	GROUP BY second, bike_id
   *  )
   *  SELECT time_bucket(interval '300000 ms', g.time) AS half_minute, AVG(t.value), t.bike_id, st_makeline(g.value::geometry) FROM gps_benchmark g, trip t
   *  WHERE g.bike_id = 'bike_10' AND g.time = t.second AND g.time > '2018-08-29 18:00:00.0' AND g.time < '2018-08-29 19:00:00.0'
   *  GROUP BY half_minute, t.bike_id;
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT time_bucket(interval '300000 ms', time) as time_bucket, AVG(s_40), st_makeline(s_12::geometry), bike_id
   *  FROM test WHERE s_40 > 3.0 AND (bike_id = 'bike_7')
   *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0')
   *  GROUP BY time_bucket, bike_id;
   * </code></p>
   * @param gpsValueRangeQuery
   * @return
   */
  @Override
  public Status gpsAggValueRangeQuery(GpsAggValueRangeQuery gpsValueRangeQuery) {
    String sql = "";
    SensorGroup plainSensorGroup = gpsValueRangeQuery.getSensorGroup();
    SensorGroup gpsSensorGroup = gpsValueRangeQuery.getGpsSensorGroup();
    DeviceSchema deviceSchema = gpsValueRangeQuery.getDeviceSchemas().get(0);

    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    Timestamp startTimestamp = new Timestamp(gpsValueRangeQuery.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(gpsValueRangeQuery.getEndTimestamp());
    if (dataModel == TableMode.NARROW_TABLE) {
      sql = "with trip as (\n" +
              "\tselect time_bucket(interval '1 s', %s) as second, %s, avg(%s) as value from %s \n" +
              "\twhere %s > %f and %s = '%s' \n" +
              "\tgroup by second, %s\n" +
              ") \n" +
              "select time_bucket(interval '%d ms', g.%s) as half_minute, avg(t.%s), t.%s, st_makeline(g.%s::geometry) from %s g, trip t \n" +
              "where g.%s = '%s' and g.%s = t.second and g.%s > '%s' and g.%s < '%s'\n" +
              "group by half_minute, t.%s;";
      sql = String.format(Locale.US, sql, timeColumn, bikeColumn, valueColumn, plainSensorGroup.getTableName(),
              valueColumn, config.QUERY_LOWER_LIMIT, bikeColumn, deviceSchema.getDevice(), bikeColumn, config.TIME_BUCKET,
              timeColumn, valueColumn, bikeColumn, valueColumn, gpsSensorGroup.getTableName(), bikeColumn,
              deviceSchema.getDevice(), timeColumn, timeColumn, startTimestamp, timeColumn, endTimestamp, bikeColumn);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = plainSensorGroup.getSensors().get(0);
      Sensor gpsSensor = gpsValueRangeQuery.getGpsSensorGroup().getSensors().get(0);
      List<String> aggregatedColumns = new ArrayList<>(Collections.singletonList(sensor.getName()));
      List<String> plainColumns = new ArrayList<>(
              Arrays.asList("st_makeline(" + gpsSensor.getName() + "::geometry)", SqlBuilder.Column.BIKE.getName())
      );
      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, config.QUERY_AGGREGATE_FUN, config.TIME_BUCKET)
              .from(tableName).where().value(sensor.getName(), SqlBuilder.Op.GREATER, gpsValueRangeQuery.getValueThreshold())
              .and().bikes(gpsValueRangeQuery.getDeviceSchemas()).and().time(gpsValueRangeQuery)
              .groupBy(Arrays.asList(Constants.TIME_BUCKET_ALIAS, SqlBuilder.Column.BIKE.getName()));
      sql = sqlBuilder.build();
    }
    return executeQueryAndGetStatus(sql);
  }

  /**
   * Scans metrics of a sensor in a specific time interval and selects those which have a certain value.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT time, bike_id, value FROM emg_benchmark WHERE value > 3.0 AND (bike_id = 'bike_5')
   *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_5')
   *  AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0') AND s_40 > 3.0;
   * </code></p>
   *
   * @param valueRangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    SensorGroup sensorGroup = valueRangeQuery.getSensorGroup();
    List<String> columns = new ArrayList<>(Arrays.asList(SqlBuilder.Column.TIME.getName(), SqlBuilder.Column.BIKE.getName()));
    if (dataModel == TableMode.NARROW_TABLE) {
      columns.addAll(sensorGroup.getFields());
      sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName()).where()
              .value(SqlBuilder.Op.GREATER, valueRangeQuery.getValueThreshold()).and()
              .bikes(valueRangeQuery.getDeviceSchemas()).and().time(valueRangeQuery);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      columns.add(sensor.getName());
      sqlBuilder = sqlBuilder.reset().select(columns).from(tableName).where().bikes(valueRangeQuery.getDeviceSchemas())
              .and().time(valueRangeQuery)
              .and().value(sensor.getName(), SqlBuilder.Op.GREATER, valueRangeQuery.getValueThreshold());
    }
    String s = sqlBuilder.build();
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Aggregates metrics of a sensor over time and groups by bike.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT AVG(value), bike_id FROM emg_benchmark WHERE (time >= '2018-08-29 18:00:00.0'
   *  AND time <= '2018-08-29 19:00:00.0') AND (bike_id = 'bike_8') GROUP BY bike_id;
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT AVG(s_40), bike_id FROM test WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0')
   *  AND (bike_id = 'bike_5') GROUP BY bike_id;
   * </code></p>
   *
   * @param aggRangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    SensorGroup sensorGroup = aggRangeQuery.getSensorGroup();

    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));

      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggRangeQuery.getAggrFunc())
              .from(aggRangeQuery.getSensorGroup().getTableName()).where().time(aggRangeQuery)
              .and().bikes(aggRangeQuery.getDeviceSchemas()).groupBy(SqlBuilder.Column.BIKE);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      List<String> aggregatedColumns = new ArrayList<>(Collections.singletonList(sensor.getName()));
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));
      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggRangeQuery.getAggrFunc())
              .from(tableName).where().time(aggRangeQuery).and().bikes(aggRangeQuery.getDeviceSchemas())
              .groupBy(SqlBuilder.Column.BIKE);
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Selects metrics based on their value, groups by bike and aggregates the values.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT AVG(value), bike_id FROM emg_benchmark WHERE value > 3.0 AND (bike_id = 'bike_10') GROUP BY bike_id;
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT AVG(s_40), bike_id FROM test WHERE s_40 > 3.0 AND (bike_id = 'bike_10') GROUP BY bike_id;
   * </code></p>
   *
   * @param aggValueQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    SensorGroup sensorGroup = aggValueQuery.getSensorGroup();
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
      List<String> plainColumns = new ArrayList<>(Collections.singleton(SqlBuilder.Column.BIKE.getName()));
      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggValueQuery.getAggrFunc())
              .from(sensorGroup.getTableName()).where().value(SqlBuilder.Op.GREATER, aggValueQuery.getValueThreshold())
              .and().bikes(aggValueQuery.getDeviceSchemas()).groupBy(SqlBuilder.Column.BIKE);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      List<String> aggregatedColumns = new ArrayList<>(Collections.singletonList(sensor.getName()));
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));
      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggValueQuery.getAggrFunc())
              .from(tableName).where().value(sensor.getName(), SqlBuilder.Op.GREATER, aggValueQuery.getValueThreshold())
              .and().bikes(aggValueQuery.getDeviceSchemas()).groupBy(SqlBuilder.Column.BIKE);
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Selects metrics based on their values in a time range, groups by bikes and aggregates the values.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT AVG(value), bike_id FROM emg_benchmark WHERE (time >= '2018-08-29 18:00:00.0'
   *  AND time <= '2018-08-29 19:00:00.0') AND value >= 3.0 AND (bike_id = 'bike_8') GROUP BY bike_id;
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT AVG(s_40), bike_id FROM test WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0')
   *  AND s_40 >= 3.0 AND (bike_id = 'bike_8') GROUP BY bike_id;
   * </code></p>
   *
   * @param aggRangeValueQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    SensorGroup sensorGroup = aggRangeValueQuery.getSensorGroup();
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));

      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggRangeValueQuery.getAggrFunc())
              .from(sensorGroup.getTableName()).where().time(aggRangeValueQuery)
              .and().value(SqlBuilder.Op.EQUALS_GREATER, aggRangeValueQuery.getValueThreshold())
              .and().bikes(aggRangeValueQuery.getDeviceSchemas()).groupBy(SqlBuilder.Column.BIKE);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      List<String> aggregatedColumns = new ArrayList<>(Collections.singletonList(sensor.getName()));
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));

      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggRangeValueQuery.getAggrFunc())
              .from(tableName).where().time(aggRangeValueQuery)
              .and().value(sensor.getName(), SqlBuilder.Op.EQUALS_GREATER, aggRangeValueQuery.getValueThreshold())
              .and().bikes(aggRangeValueQuery.getDeviceSchemas()).groupBy(SqlBuilder.Column.BIKE);
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
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
   *  SELECT time_bucket(interval '300000 ms', time) as time_bucket, AVG(s_40), bike_id FROM test
   *  WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0') GROUP BY time_bucket, bike_id;
   * </code></p>
   *
   * @param groupByQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    SensorGroup sensorGroup = groupByQuery.getSensorGroup();
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));
      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, groupByQuery.getAggrFunc(), config.TIME_BUCKET)
              .from(sensorGroup.getTableName()).where().time(groupByQuery)
              .groupBy(Arrays.asList(Constants.TIME_BUCKET_ALIAS, SqlBuilder.Column.BIKE.getName()));
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      List<String> aggregatedColumns = new ArrayList<>(Collections.singletonList(sensor.getName()));
      List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));
      sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, groupByQuery.getAggrFunc(), config.TIME_BUCKET)
              .from(tableName).where().time(groupByQuery)
              .groupBy(Arrays.asList(Constants.TIME_BUCKET_ALIAS, SqlBuilder.Column.BIKE.getName()));
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Selects the latest metric of a sensor.
   *
   * NARROW_TABLE:
   * <p><code>
   *  SELECT value, bike_id, time, sensor_id FROM emg_benchmark WHERE (bike_id = 'bike_5') ORDER BY time DESC LIMIT 1;
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  SELECT time, bike_id, s_40 FROM test WHERE (bike_id = 'bike_8') ORDER BY time DESC LIMIT 1;
   * </code></p>
   *
   * @param latestPointQuery The query parameters query.
   * @return The status of the execution.
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    SensorGroup sensorGroup = latestPointQuery.getSensorGroup();
    if (dataModel == TableMode.NARROW_TABLE) {
      List<String> columns = new ArrayList<>(sensorGroup.getSensors().get(0).getFields());
      columns.addAll(Arrays.asList(SqlBuilder.Column.BIKE.getName(), SqlBuilder.Column.TIME.getName(),
              SqlBuilder.Column.SENSOR.getName()));
      sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName()).where()
              .bikes(latestPointQuery.getDeviceSchemas())
              .orderBy(SqlBuilder.Column.TIME.getName(), SqlBuilder.Order.DESC).limit(1);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      List<String> columns = new ArrayList<>(Arrays.asList(SqlBuilder.Column.TIME.getName(),
              SqlBuilder.Column.BIKE.getName(), sensor.getName()));
      sqlBuilder = sqlBuilder.reset().select(columns).from(tableName).where().bikes(latestPointQuery.getDeviceSchemas())
              .orderBy(SqlBuilder.Column.TIME.getName(), SqlBuilder.Order.DESC).limit(1);
    }
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Creates a heat map with average air quality out of gps points.
   *
   * NARROW_TABLE:
   * <p><code>
   *  with map as (select (st_dump(map.geom)).geom from (
   * 	select st_setsrid(st_collect(grid.geom),4326) as geom from ST_CreateGrid(40, 90, 0.0006670, 0.0006670, 13.410947, 48.556736) as grid
   *  ) map)
   *  select m.geom as cell, avg(a.value) from gps_benchmark g, map m, (
   * 	select time_bucket(interval '1 sec', ab.time) as second, avg(value) as value, bike_id from emg_benchmark ab where ab.time > '2018-08-29 18:00:00.0' and ab.time < '2018-08-29 19:00:00.0' group by second, bike_id
   *  ) a
   *  where g.bike_id = a.bike_id and a.second = g.time and st_contains(m.geom, g.value::geometry) group by m.geom;
   * </code></p>
   *
   * WIDE_TABLE:
   * <p><code>
   *  with map as (select (st_dump(map.geom)).geom from (
   * 	select st_setsrid(st_collect(grid.geom),4326) as geom from ST_CreateGrid(40, 90, 0.0006670, 0.0006670, 13.410947, 48.556736) as grid
   *  ) map)
   *  select avg(s_40), m.geom from test t inner join map m on st_contains(m.geom, t.s_12::geometry) where t.s_12 is not null and g.time > '2018-08-29 18:00:00.0' and g.time < '2018-08-29 19:00:00.0' group by m.geom;
   * </code></p>
   * @param gpsRangeQuery The heatmap query paramters object.
   * @return The status of the execution.
   */
  @Override
  public Status heatmapRangeQuery(GpsValueRangeQuery gpsRangeQuery) {
    SensorGroup sensorGroup = gpsRangeQuery.getSensorGroup();
    SensorGroup gpsSensorGroup = gpsRangeQuery.getGpsSensorGroup();
    Timestamp startTimestamp = new Timestamp(gpsRangeQuery.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(gpsRangeQuery.getEndTimestamp());
    GeoPoint startPoint = Constants.GRID_START_POINT;
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      sql = "with map as (select (st_dump(map.geom)).geom from (\n" +
              "\tselect st_setsrid(st_collect(grid.geom),4326) as geom from ST_CreateGrid(40, 90, 0.0006670, 0.0006670,"
              + " %f, %f) as grid\n"
              + ") map)\n"
              + "select m.geom as cell, avg(a.%s) from %s g, map m, (\n"
              + "\tselect time_bucket(interval '1 sec', ab.%s) as second, avg(%s) as value, %s from %s ab "
              + "where ab.%s > '%s' and ab.%s < '%s' group by second, %s\n"
              + ") a\n"
              + "where g.%s = a.%s and a.second = g.time and st_contains(m.geom, g.value::geometry) group by m.geom;";
      sql = String.format(Locale.US, sql, startPoint.getLongitude(), startPoint.getLatitude(), valueColumn,
              gpsSensorGroup.getTableName(), timeColumn, valueColumn, bikeColumn, sensorGroup.getTableName(), timeColumn,
              startTimestamp, timeColumn, endTimestamp, bikeColumn, bikeColumn, bikeColumn);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = sensorGroup.getSensors().get(0);
      Sensor gpsSensor = gpsSensorGroup.getSensors().get(0);
      sql = "with map as (select (st_dump(map.geom)).geom from (\n" +
              "\tselect st_setsrid(st_collect(grid.geom),4326) as geom from ST_CreateGrid(40, 90, 0.0006670, 0.0006670, %f, %f) as grid\n"
              + ") map)\n"
              + "select avg(%s), m.geom from %s t inner join map m on st_contains(m.geom, t.%s::geometry) where t.%s is not null "
              + "and t.%s > '%s' and t.%s < '%s' group by m.geom;";
      sql = String.format(Locale.US, sql, startPoint.getLongitude(), startPoint.getLatitude(), sensor.getName(),
              tableName, gpsSensor.getName(), gpsSensor.getName(), timeColumn, startTimestamp, timeColumn, endTimestamp);
    }
    return executeQueryAndGetStatus(sql);
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
   *  with trip as (
   * 	select time_bucket(interval '1000 ms', time) as second, avg(s_40), bike_id, s_12 from test where s_40 > 3.000000 and bike_id ='bike_7' and s_12 is not null group by second, bike_id, s_12
   *  ) select st_length(st_makeline(s_12::geometry)::geography, false) from trip where second > '2018-08-29 18:00:00.0' and second < '2018-08-29 19:00:00.0';
   * </code></p>
   *
   * @param gpsRangeQueryRangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status distanceRangeQuery(GpsValueRangeQuery gpsRangeQueryRangeQuery) {
    Timestamp startTimestamp = new Timestamp(gpsRangeQueryRangeQuery.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(gpsRangeQueryRangeQuery.getEndTimestamp());
    String sql = "";
    String valueColumn = SqlBuilder.Column.VALUE.getName();
    String bikeColumn = SqlBuilder.Column.BIKE.getName();
    String timeColumn = SqlBuilder.Column.TIME.getName();
    DeviceSchema deviceSchema = gpsRangeQueryRangeQuery.getDeviceSchemas().get(0);
    SensorGroup plainSensorGroup = gpsRangeQueryRangeQuery.getSensorGroup();
    SensorGroup gpsSensorGroup = gpsRangeQueryRangeQuery.getGpsSensorGroup();
    if (dataModel == TableMode.NARROW_TABLE) {
      sql = "with trip_begin as (\n" +
            "\tselect time_bucket(interval '1 s', %s) as second, %s from %s where %s > %f and %s = '%s' and %s > '%s' and %s < '%s'group by second, %s order by second asc limit 1\n" +
            "), trip_end as (\n" +
            "\tselect time_bucket(interval '1 s', %s) as second, %s from %s where %s > %f and %s = '%s' and %s > '%s'and %s < '%s' group by second, %s order by second desc limit 1\n" +
            ")\n" +
            "select st_length(st_makeline(g.%s::geometry)::geography, false) from %s g, trip_begin b, trip_end e where g.%s = '%s'\n" +
            "and g.time > b.second and g.time < e.second group by g.bike_id;";
      sql = String.format(Locale.US, sql, timeColumn, bikeColumn, plainSensorGroup.getTableName(), valueColumn,
              gpsRangeQueryRangeQuery.getValueThreshold(), bikeColumn, deviceSchema.getDevice(), timeColumn, startTimestamp,
              timeColumn, endTimestamp, bikeColumn, timeColumn, bikeColumn, plainSensorGroup.getTableName(), valueColumn,
              gpsRangeQueryRangeQuery.getValueThreshold(), bikeColumn, deviceSchema.getDevice(), timeColumn, startTimestamp,
              timeColumn, endTimestamp, bikeColumn, valueColumn, gpsSensorGroup.getTableName(), bikeColumn,
              deviceSchema.getDevice());
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor sensor = plainSensorGroup.getSensors().get(0);
      Sensor gpsSensor = gpsSensorGroup.getSensors().get(0);
      sql = "with trip as (\n" +
              "\tselect time_bucket(interval '%d ms', %s) as second, avg(%s), %s, %s from %s where %s > %f and %s ='%s' and %s is not null group by second, %s, %s\n" +
              ") select st_length(st_makeline(s_12::geometry)::geography, false) from trip where second > '%s' and second < '%s';";
      sql = String.format(Locale.US, sql, gpsSensor.getInterval(), SqlBuilder.Column.TIME.getName(), sensor.getName(),
              SqlBuilder.Column.BIKE.getName(), gpsSensor.getName(), tableName, sensor.getName(),
              gpsRangeQueryRangeQuery.getValueThreshold(), SqlBuilder.Column.BIKE.getName(), deviceSchema.getDevice(),
              gpsSensor.getName(), SqlBuilder.Column.BIKE.getName(), gpsSensor.getName(), startTimestamp.toString(),
              endTimestamp.toString());
    }
    return executeQueryAndGetStatus(sql);
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
   *  with last_location as (
   * 	select bike_id, last(s_12, time) as location from test where s_12 is not null group by bike_id
   *  ) select * from last_location l
   *  where st_contains(
   *    st_buffer(st_setsrid(st_makepoint(13.431947, 48.566736),4326)::geography, 500)::geometry,
   *    l.location::geometry
   *  );
   * </code></p>
   * @param gpsRangeQuery
   * @return
   */
  @Override
  public Status bikesInLocationQuery(GpsRangeQuery gpsRangeQuery) {
    String sql = "";
    if (dataModel == TableMode.NARROW_TABLE) {
      SensorGroup gpsSensorGroup = gpsRangeQuery.getGpsSensorGroup();
      sql = "with last_location as (\n" +
              "\tselect %s, last(%s, %s) as location from %s group by %s\n" +
              ") select * from last_location l where st_contains(st_buffer(st_setsrid(st_makepoint(%f, %f),4326)::geography, %d)::geometry, l.location::geometry);";
      sql = String.format(Locale.US, sql, SqlBuilder.Column.BIKE.getName(), SqlBuilder.Column.VALUE.getName(),
              SqlBuilder.Column.TIME.getName(), gpsSensorGroup.getTableName(), SqlBuilder.Column.BIKE.getName(),
              Constants.SPAWN_POINT.getLongitude(), Constants.SPAWN_POINT.getLatitude(), config.RADIUS);
    } else if (dataModel == TableMode.WIDE_TABLE) {
      Sensor gpsSensor = gpsRangeQuery.getGpsSensorGroup().getSensors().get(0);
      sql = "with last_location as (\n" +
              "\tselect %s, last(%s, %s) as location from %s where %s is not null group by %s\n" +
              ") select * from last_location l where st_contains(st_buffer(st_setsrid(st_makepoint(%f, %f),4326)::geography, %d)::geometry, l.location::geometry);";
      sql = String.format(Locale.US, sql, SqlBuilder.Column.BIKE.getName(), gpsSensor.getName(), SqlBuilder.Column.TIME.getName(),
              tableName, gpsSensor.getName(), SqlBuilder.Column.BIKE.getName(), Constants.SPAWN_POINT.getLongitude(),
              Constants.SPAWN_POINT.getLatitude(), config.RADIUS);
    }
    return executeQueryAndGetStatus(sql);
  }

  /*
   * Executes the SQL query and measures the execution time.
   */
  private Status executeQueryAndGetStatus(String sql) {
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
   * Returns an SQL statement, which creates a narrow table for a single sensor group.
   *
   * TODO change sql
   * <p>
   * CREATE TABLE group_0 (time TIMESTAMPTZ NOT NULL, group_id TEXT NOT NULL, bike_id TEXT NOT NULL,
   * s_0 DOUBLE PRECISION NULL, s_1 DOUBLE PRECISION NULL);
   * </p>
   */
  private String getCreateTableSql(SensorGroup sensorGroup) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE ").append(sensorGroup.getTableName()).append(" (time TIMESTAMPTZ NOT NULL, "
            + "bike_id VARCHAR(20) REFERENCES bikes (bike_id), sensor_id VARCHAR(20) NOT NULL");

    sensorGroup.getFields().stream().forEach(field -> sqlBuilder.append(", ").append(field).append(" ")
            .append(sensorGroup.getDataType()).append(" NULL"));
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  /*
   * Returns an SQL statement, which creates a wide table for all sensors at once.
   */
  private String getCreateTableSql() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE ").append(tableName).append(" (time TIMESTAMPTZ NOT NULL, ")
            .append("bike_id VARCHAR(20) REFERENCES bikes (bike_id)");
    config.SENSORS.forEach(sensor -> sqlBuilder.append(", ").append(sensor.getName()).append(" ")
            .append(sensor.getDataType()));
    sqlBuilder.append(");");
    return sqlBuilder.toString();
  }

  /*
   * Returns an SQL query for creating the bikes meta table.
   */
  private String getCreateBikesTableSql() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE bikes (bike_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, date_manufactured TIMESTAMPTZ);");
    return sqlBuilder.toString();
  }

  /*
   * Returns an SQL batch insert query for insert bikes meta data.
   */
  private String getInsertBikesSql(List<DeviceSchema> schemaList) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("INSERT INTO bikes (bike_id, name, date_manufactured) VALUES ");

    boolean firstIteration = true;
    for (int i = 0; i < schemaList.size(); i++) {
      if (firstIteration) {
        firstIteration = false;
      } else {
        sqlBuilder.append(", ");
      }

      DeviceSchema schema = schemaList.get(i);
      sqlBuilder.append("('").append(schema.getDevice()).append("', ").append("'name_").append(i).append("', ")
              .append("now()").append(")");
    }
    sqlBuilder.append(";");
    return sqlBuilder.toString();
  }

  /*
   * Creates a list of SQL batch queries. Each string writes points to a distinct narrow table.
   */
  private List<String> getInsertOneBatchSql(Batch batch) {
    Map<Sensor, Point[]> entries = batch.getEntries();
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    StringBuilder sqlBuilder = new StringBuilder();

    List<String> sensorQueries = new ArrayList<>(deviceSchema.getSensors().size());
    for (Sensor sensor : deviceSchema.getSensors()) {
      if (entries.get(sensor).length == 0) {
        continue;
      }

      sqlBuilder.append("INSERT INTO ").append(sensor.getSensorGroup().getTableName()).append(" VALUES ");

      boolean firstIteration = true;
      for (Point point : entries.get(sensor)) {
        if (firstIteration) {
          firstIteration = false;
        } else {
          sqlBuilder.append(", ");
        }
        Timestamp timestamp = new Timestamp(point.getTimestamp());
        sqlBuilder.append("('").append(timestamp).append("', ")
                .append("'").append(deviceSchema.getDevice()).append("', ")
                .append("'").append(sensor.getName()).append("'");
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

  /*
   * Returns an SQL statement, which inserts a batch of points into single wide table.
   */
  private String getInsertOneWideBatchSql(Batch batch) {
    Map<Long, List<String>> rows = transformBatch(batch);
    StringBuilder sqlBuilder = new StringBuilder();
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    sqlBuilder.append("INSERT INTO ").append(tableName).append(" VALUES ");
    boolean firstIteration = true;
    for (long t : rows.keySet()) {
      if (firstIteration) {
        firstIteration = false;
      } else {
        sqlBuilder.append(", ");
      }
      Timestamp timestamp = new Timestamp(t);
      sqlBuilder.append("('").append(timestamp).append("', '").append(deviceSchema.getDevice()).append("'");
      List<String> valuesList = rows.get(t);
      valuesList.forEach(value -> sqlBuilder.append(", ").append(value));
      sqlBuilder.append(")");
    }
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
