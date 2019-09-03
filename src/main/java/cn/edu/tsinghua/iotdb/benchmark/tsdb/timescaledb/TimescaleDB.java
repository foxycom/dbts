package cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
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

  /**
   * Initializes an instance of the database controller.
   */
  public TimescaleDB() {
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

      String findSensorTablesSql = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' and " +
              "tablename LIKE '%benchmark'";
      try (ResultSet rs = statement.executeQuery(findSensorTablesSql)) {

        while (rs.next()) {
          statement.addBatch(String.format(DROP_TABLE, rs.getString("tablename")));
        }
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
      for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
        String createIndexOnBikeSql = "CREATE INDEX ON " + sensorGroup.getTableName() + " (bike_id, time DESC);";
        String createIndexOnBikeAndSensorSql = "CREATE INDEX ON " + sensorGroup.getTableName() + " (bike_id, sensor_id, time DESC)";
        statement.addBatch(createIndexOnBikeSql);
        statement.addBatch(createIndexOnBikeAndSensorSql);
      }

      statement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("Could not create PG indexes because: {}", e.getMessage());
    }
  }

  /*
   * Registers an SQL function in SQL, which generates a grid for a heat map.
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
    List<String> sqlQueries;
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      sqlQueries = getInsertOneBatchSql(batch);
      for (String query : sqlQueries) {
        statement.addBatch(query);
      }
      st = System.nanoTime();
      statement.executeBatch();
      connection.commit();
      en = System.nanoTime();
      return new Status(true, en - st);
    } catch (SQLException e) {
      System.out.println(e.getNextException());
      return new Status(false, 0, e, e.toString());
    }
  }

  /**
   * TODO: READY TO USE
   *
   * Selects data points, which are stored with a given timestamp.
   *
   * SELECT value, time, bike_id FROM emg_benchmark
   * WHERE (bike_id = 'bike_0' OR bike_id = 'bike_3' OR bike_id = 'bike_2') AND (time = '2018-08-29 18:00:00.0');
   *
   * @param preciseQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    SensorGroup sensorGroup = preciseQuery.getSensorGroup();
    List<String> columns = new ArrayList<>(sensorGroup.getFields());
    columns.addAll(Arrays.asList("time", "bike_id"));
    sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName())
            .where().bikes(preciseQuery.getDeviceSchema()).and().time(preciseQuery);

    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Executes a scan of values over a given period of time on a given number of bikes.
   *
   * SELECT value FROM emg_benchmark WHERE (bike_id = 'bike_0' OR bike_id = 'bike_3' OR bike_id = 'bike_2')
   * AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
   *
   * @param rangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    List<String> columns = new ArrayList<>();
    columns.add("value");

    sqlBuilder = sqlBuilder.reset().select(columns).from(rangeQuery.getSensorGroup().getTableName())
            .where().bikes(rangeQuery.getDeviceSchema()).and().time(rangeQuery);
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Performs a scan of gps data points in a given time range for specified number of bikes.
   *
   * SELECT value FROM gps_benchmark WHERE (bike_id = 'bike_1')
   * AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
   *
   * @param rangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status gpsRangeQuery(RangeQuery rangeQuery) {
    SensorGroup sensorGroup = rangeQuery.getSensorGroup();
    List<String> columns = Collections.unmodifiableList(sensorGroup.getSensors().get(0).getFields());

    sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName())
            .where().bikes(rangeQuery.getDeviceSchema()).and().time(rangeQuery);
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Identifies active trips, when current value is above a threshold.
   *
   * WITH trip (second, current_value) AS (SELECT time_bucket('1 second', time) AS second, AVG(value)
   *    FROM current_benchmark WHERE bike_id = 'bike_1' and time > '2019-08-19 18:00:00.000'
   *    and time < '2019-08-20 18:00:00.000' GROUP BY second HAVING AVG(value) > 0.1
   * ) SELECT g.time, t.current_value, g.value as location FROM gps_benchmark g INNER JOIN trip t ON g.time = t.second
   * WHERE g.bike_id = 'bike_1';
   *
   * @param rangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status gpsValueRangeQuery(ValueRangeQuery rangeQuery) {
    DeviceSchema deviceSchema = rangeQuery.getDeviceSchema().get(0);
    double threshold = rangeQuery.getValueThreshold();

    Timestamp startTimestamp = new Timestamp(Constants.START_TIMESTAMP);
    Timestamp endTimestamp = new Timestamp(Constants.START_TIMESTAMP + config.QUERY_INTERVAL);
    String sql = "WITH trip (second, current_value) AS (SELECT time_bucket('1 second', time) AS second, AVG(value)"
            + " FROM current_benchmark WHERE bike_id = '" + deviceSchema.getDevice() + "' and time > '" + startTimestamp
            + "' and time < '" + endTimestamp + "' GROUP BY second HAVING AVG(value) > " + threshold + ")"
            + " SELECT g.time, t.current_value, g.value as location FROM gps_benchmark g INNER JOIN trip t "
            + "ON g.time = t.second WHERE g.bike_id = '" + deviceSchema.getDevice() + "';";
    return executeQueryAndGetStatus(sql);
  }

  /**
   * TODO: READY TO USE
   *
   * Scans metrics of a sensor in a specific time interval and selects those which have a certain value.
   *
   * SELECT value, bike_id, time FROM emg_benchmark
   * WHERE value > -5.0 AND (bike_id = 'bike_2' OR bike_id = 'bike_3' OR bike_id = 'bike_0')
   * AND (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0');
   *
   * @param valueRangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    SensorGroup sensorGroup = valueRangeQuery.getSensorGroup();
    List<String> columns = new ArrayList<>(sensorGroup.getFields());
    columns.addAll(Arrays.asList("bike_id", "time"));
    sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName()).where()
            .value(SqlBuilder.Op.GREATER, valueRangeQuery.getValueThreshold()).and()
            .bikes(valueRangeQuery.getDeviceSchema()).and().time(valueRangeQuery);
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Aggregates metrics of a sensor over time and groups by bike.
   *
   * SELECT SUM(value), bike_id FROM emg_benchmark
   * WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0')
   * AND (bike_id = 'bike_2' OR bike_id = 'bike_0' OR bike_id = 'bike_3') GROUP BY bike_id;
   *
   * @param aggRangeQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    SensorGroup sensorGroup = aggRangeQuery.getSensorGroup();
    List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
    List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));

    sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggRangeQuery.getAggrFunc())
            .from(aggRangeQuery.getSensorGroup().getTableName()).where().time(aggRangeQuery)
            .and().bikes(aggRangeQuery.getDeviceSchema()).groupBy(SqlBuilder.Column.BIKE);
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Selects metrics based on their value, groups by bike and aggregates the values.
   *
   * SELECT SUM(value), bike_id FROM emg_benchmark
   * WHERE value > -5.0 AND (bike_id = 'bike_3' OR bike_id = 'bike_2' OR bike_id = 'bike_0') GROUP BY bike_id;
   *
   * @param aggValueQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    SensorGroup sensorGroup = aggValueQuery.getSensorGroup();
    List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
    List<String> plainColumns = new ArrayList<>(Collections.singleton(SqlBuilder.Column.BIKE.getName()));
    sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggValueQuery.getAggrFunc())
            .from(sensorGroup.getTableName()).where().value(SqlBuilder.Op.GREATER, aggValueQuery.getValueThreshold())
            .and().bikes(aggValueQuery.getDeviceSchema()).groupBy(SqlBuilder.Column.BIKE);
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Selects metrics based on their values in a time range, groups by bikes and aggregates the values.
   *
   * SELECT SUM(value), bike_id FROM emg_benchmark
   * WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0')
   * AND value >= -5.0 AND (bike_id = 'bike_2' OR bike_id = 'bike_0' OR bike_id = 'bike_3') GROUP BY bike_id;
   *
   * @param aggRangeValueQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    SensorGroup sensorGroup = aggRangeValueQuery.getSensorGroup();
    List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
    List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));

    sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, aggRangeValueQuery.getAggrFunc())
            .from(sensorGroup.getTableName()).where().time(aggRangeValueQuery)
            .and().value(SqlBuilder.Op.EQUALS_GREATER, aggRangeValueQuery.getValueThreshold())
            .and().bikes(aggRangeValueQuery.getDeviceSchema()).groupBy(SqlBuilder.Column.BIKE);
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO test
   *
   * Groups entries within a time range in time buckets and by bikes and aggregates values in each time bucket.
   *
   * SELECT time_bucket(interval '60000 ms', time) as time_bucket, SUM(value), bike_id FROM emg_benchmark
   * WHERE (time >= '2018-08-29 18:00:00.0' AND time <= '2018-08-29 19:00:00.0') GROUP BY time_bucket, bike_id;
   *
   * @param groupByQuery The query parameters object.
   * @return The status of the execution.
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    SensorGroup sensorGroup = groupByQuery.getSensorGroup();
    List<String> aggregatedColumns = new ArrayList<>(sensorGroup.getFields());
    List<String> plainColumns = new ArrayList<>(Collections.singletonList(SqlBuilder.Column.BIKE.getName()));
    sqlBuilder = sqlBuilder.reset().select(aggregatedColumns, plainColumns, groupByQuery.getAggrFunc(), config.TIME_BUCKET)
            .from(sensorGroup.getTableName()).where().time(groupByQuery)
            .groupBy(Arrays.asList(Constants.TIME_BUCKET_ALIAS, SqlBuilder.Column.BIKE.getName()));
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * TODO: READY TO USE
   *
   * Selects the latest metric of a sensor.
   *
   * SELECT value, bike_id, time, sensor_id FROM emg_benchmark
   * WHERE (bike_id = 'bike_0' OR bike_id = 'bike_3' OR bike_id = 'bike_2') ORDER BY time DESC LIMIT 1;
   *
   * @param latestPointQuery The query parameters query.
   * @return The status of the execution.
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    List<DeviceSchema> deviceSchemas = latestPointQuery.getDeviceSchema();
    SensorGroup sensorGroup = latestPointQuery.getSensorGroup();
    List<String> columns = new ArrayList<>(sensorGroup.getSensors().get(0).getFields());
    columns.addAll(Arrays.asList("bike_id", "time", "sensor_id"));
    sqlBuilder = sqlBuilder.reset().select(columns).from(sensorGroup.getTableName()).where().bikes(deviceSchemas)
            .orderBy("time", SqlBuilder.Order.DESC).limit(1);
    String debug = sqlBuilder.build();
    return executeQueryAndGetStatus(sqlBuilder.build());
  }

  /**
   * Creates a heat map with average air quality out of gps points.
   *
   * @param heatmapRangeQuery The heatmap query paramters object.
   * @return The status of the execution.
   */
  @Override
  public Status heatmapRangeQuery(HeatmapRangeQuery heatmapRangeQuery) {
    SensorGroup sensorGroup = heatmapRangeQuery.getSensorGroup();
    SensorGroup gpsSensorGroup = heatmapRangeQuery.getGpsSensorGroup();
    GeoPoint startPoint = Constants.GRID_START_POINT;
    String sql = "with map as (select (st_dump(map.geom)).geom from (\n" +
            "\tselect st_setsrid(st_collect(grid.geom),4326) as geom from ST_CreateGrid(40, 90, 0.0006670, 0.0006670, "
            + startPoint.getLongitude() + ", " + startPoint.getLatitude() + ") as grid\n"
            + ") map)\n"
            + "select m.geom as cell, avg(a.value) from " + gpsSensorGroup.getTableName() + " g, map m, (\n"
            + "\tselect time_bucket(interval '1 sec', ab.time) as second, avg(value) as value, bike_id from airquality_benchmark ab group by second, bike_id\n"
            + ") a\n"
            + "where g.bike_id = a.bike_id and a.second = g.time and st_contains(m.geom, g.value::geometry) group by m.geom;";
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

  /**
   * -- Creating a regular SQL table example.
   * <p>
   * CREATE TABLE group_0 (time TIMESTAMPTZ NOT NULL, group_id TEXT NOT NULL, bike_id TEXT NOT NULL,
   * s_0 DOUBLE PRECISION NULL, s_1 DOUBLE PRECISION NULL);
   * </p>
   * @return create table SQL String
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
   * Creates a list of SQL batch queries. Each string is writes points to a distinct sensor group (table).
   */
  private List<String> getInsertOneBatchSql(Batch batch) {
    Map<Sensor, Point[]> entries = batch.getEntries();
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    StringBuilder sb = new StringBuilder();

    List<String> sensorQueries = new ArrayList<>(deviceSchema.getSensors().size());
    for (Sensor sensor : deviceSchema.getSensors()) {
      if (entries.get(sensor).length == 0) {
        continue;
      }

      sb.append("INSERT INTO ").append(sensor.getSensorGroup().getTableName()).append(" VALUES ");

      boolean firstIteration = true;
      for (Point point : entries.get(sensor)) {
        if (firstIteration) {
          firstIteration = false;
        } else {
          sb.append(", ");
        }
        Timestamp timestamp = new Timestamp(point.getTimestamp());
        sb.append("('").append(timestamp).append("', ")
                .append("'").append(deviceSchema.getDevice()).append("', ")
                .append("'").append(sensor.getName()).append("'");
        if (sensor.getFields().size() == 1) {
          sb.append(", ").append(point.getValue());
        } else {
          String[] values = point.getValues();
          Arrays.stream(values).forEach(value -> sb.append(", ").append(value));
        }
        sb.append(")");
      }
      sb.append(";");

      sensorQueries.add(sb.toString());
      sb.setLength(0);
    }

    return sensorQueries;
  }
}
