package cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Point;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  //chunk_time_interval=7d
  private static final String CONVERT_TO_HYPERTABLE =
      "SELECT create_hypertable('%s', 'time', chunk_time_interval => interval '1 day');";
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS %s CASCADE;";
  private long initialDbSize;

  public TimescaleDB() {
    config = ConfigParser.INSTANCE.config();
    tableName = config.DB_NAME;
  }

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

        // wait for deletion complete
        LOGGER.info("Waiting {}ms for old data deletion.", config.ERASE_WAIT_TIME);
        Thread.sleep(config.ERASE_WAIT_TIME);

        initialDbSize = getInitialSize();
      }
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
   * Map the data schema concepts as follow:
   * <ul>
   * <li>DB_NAME -> table name</li>
   * <li>storage group name -> a field in table</li>
   * <li>device name -> a field in table</li>
   * <li>sensors -> fields in table</li>
   * </ul>
   * <p> Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables</p>
   * -- We start by creating a regular SQL table
   * <p><code>
   * CREATE TABLE conditions ( time        TIMESTAMPTZ       NOT NULL, location    TEXT
   * NOT NULL, temperature DOUBLE PRECISION  NULL, humidity    DOUBLE PRECISION  NULL );
   * </code></p>
   * -- This creates a hypertable that is partitioned by time using the values in the `time` column.
   * <p><code>SELECT create_hypertable('conditions', 'time');</code></p>
   */
  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);

      // Creates bikes relational data table
      String createBikesTableSql = getCreateBikesTableSql();
      statement.addBatch(createBikesTableSql);

      // Insert all bikes
      String insertBikesSql = getInsertBikesSql(schemaList);
      statement.addBatch(insertBikesSql);

      for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
        String createTableSql = getCreateTableSql(sensorGroup);
        statement.addBatch(createTableSql);
      }

      statement.executeBatch();
      connection.commit();

      for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
        String convertToHyperTableSql = getConvertToHypertableSql(sensorGroup);
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
   * Returns the size of the benchmarked database in GB.
   *
   * @return The size of the benchmarked database, i. e., 'test'.
   * @throws TsdbException
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

  private void createIndexes() {
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      for (SensorGroup sensorGroup : config.SENSOR_GROUPS) {
        String createIndexOnBikeSql = createIndexOnBike(sensorGroup.getTableName());
        String createIndexOnBikeAndSensorSql = createIndexOnBikeAndSensor(sensorGroup.getTableName());
        statement.addBatch(createIndexOnBikeSql);
        statement.addBatch(createIndexOnBikeAndSensorSql);
      }
      statement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      LOGGER.error("Could not create PG indexes because: {}", e.getMessage());
    }
  }

  private String createIndexOnBike(String tableName) {
    String createIndexSql = "CREATE INDEX ON " + tableName + " (bike_id, time DESC);";
    return createIndexSql;
  }

  private String createIndexOnBikeAndSensor(String tableName) {
    String createIndexSql = "CREATE INDEX ON " + tableName + " (bike_id, sensor_id, time DESC)";
    return createIndexSql;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    List<String> sqlQueries;
    try (Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      sqlQueries = getInsertOneBatchSql(batch);
      batch = null;
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
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and time=1535558400000.
   *
   * @param preciseQuery universal precise query condition parameters
   */
  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    int sensorNum = preciseQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(preciseQuery.getDeviceSchema());
    builder.append(" AND time = ").append(preciseQuery.getTimestamp());
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') AND (time >= 1535558400000 AND
   * time <= 1535558650000).
   *
   * @param rangeQuery universal range query condition parameters
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    int sensorNum = rangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(rangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, rangeQuery);
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  @Override
  public Status gpsPathRangeQuery(RangeQuery rangeQuery) {
    List<String> columns = new ArrayList<>(1);
    columns.add("gps");

    List<Sensor> sensors = rangeQuery.getDeviceSchema().get(0).getSensors();
    StringBuilder sqlBuilder = getSelectQueryHead(columns, sensors.get(sensors.size() - 1));
    addWhereBikeClause(sqlBuilder, rangeQuery);
    addWhereTimeClause(sqlBuilder, rangeQuery);
    String debug = sqlBuilder.toString();
    return executeQueryAndGetStatus(sqlBuilder.toString(), 1);
  }

  private void addWhereBikeClause(StringBuilder sqlBuilder, RangeQuery rangeQuery) {
    DeviceSchema bikeSchema = rangeQuery.getDeviceSchema().get(0);
    sqlBuilder.append(" WHERE bike_id = '").append(bikeSchema.getDevice()).append("'");
  }

  private StringBuilder getSelectQueryHead(List<String> columns, Sensor sensor) {
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    boolean firstIteration = true;
    for (String column : columns) {
      if (firstIteration) {
        firstIteration = false;
      } else {
        sqlBuilder.append(", ");
      }
      sqlBuilder.append(column);
    }

    // TODO fix
    sqlBuilder.append(" FROM ").append("SOMENAME");
    return sqlBuilder;
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and (s_2 > 78).
   *
   * @param valueRangeQuery contains universal range query with value filter parameters
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    int sensorNum = valueRangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(valueRangeQuery.getDeviceSchema());
    addWhereValueClause(valueRangeQuery.getDeviceSchema(), builder,
        valueRangeQuery.getValueThreshold());
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
   * time <= 1535558650000) GROUP BY device.
   *
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    int sensorNum = aggRangeQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
        aggRangeQuery.getAggFun());
    addWhereTimeClause(builder, aggRangeQuery);
    builder.append("GROUP BY device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (s_2>10) GROUP BY device.
   *
   * @param aggValueQuery contains universal aggregation query with value filter parameters
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    int sensorNum = aggValueQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
        aggValueQuery.getAggFun());
    addWhereValueClause(aggValueQuery.getDeviceSchema(), builder,
        aggValueQuery.getValueThreshold());
    builder.append(" GROUP BY device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  /**
   * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
   * time <= 1535558650000) AND (s_2>10) GROUP BY device.
   *
   * @param aggRangeValueQuery contains universal aggregation query with time and value filters
   * parameters
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    int sensorNum = aggRangeValueQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
        aggRangeValueQuery.getAggFun());
    addWhereTimeClause(builder, aggRangeValueQuery);
    addWhereValueClause(aggRangeValueQuery.getDeviceSchema(), builder,
        aggRangeValueQuery.getValueThreshold());
    builder.append("GROUP BY device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  /**
   * eg. SELECT time_bucket(5000, time) AS sampleTime, device, count(s_2) FROM tutorial WHERE
   * (device='d_2') AND (time >= 1535558400000 and time <= 1535558650000) GROUP BY time, device.
   *
   * @param groupByQuery contains universal group by query condition parameters
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    int sensorNum = groupByQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getGroupByQuerySqlHead(groupByQuery.getDeviceSchema(),
        groupByQuery.getAggFun(), groupByQuery.getGranularity());
    addWhereTimeClause(builder, groupByQuery);
    builder.append(" GROUP BY time, device");
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  /**
   * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') ORDER BY time DESC LIMIT 1. The
   * last and first commands do not use indexes, and instead perform a sequential scan through their
   * groups. They are primarily used for ordered selection within a GROUP BY aggregate, and not as
   * an alternative to an ORDER BY time DESC LIMIT 1 clause to find the latest value (which will use
   * indexes).
   *
   * @param latestPointQuery contains universal latest point query condition parameters
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    int sensorNum = latestPointQuery.getDeviceSchema().get(0).getSensors().size();
    StringBuilder builder = getSampleQuerySqlHead(latestPointQuery.getDeviceSchema());
    builder.append("ORDER BY time DESC LIMIT 1");
    return executeQueryAndGetStatus(builder.toString(), sensorNum);
  }

  private Status executeQueryAndGetStatus(String sql, int sensorNum) {
    LOGGER.info("{} 提交执行的查询SQL: {}", Thread.currentThread().getName(), sql);
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
      queryResultPointNum = line * sensorNum;
      return new Status(true, en - st, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, 0, queryResultPointNum, e, sql);
    }
  }

  /**
   * 创建查询语句--(带有聚合函数的查询) .
   * SELECT device, avg(cpu) FROM metrics WHERE (device='d_1' OR device='d_2')
   */
  private StringBuilder getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT device");
    addFunSensor(aggFun, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }

  /**
   * 创建查询语句--(带有GroupBy函数的查询) .
   * SELECT time_bucket(5, time) AS sampleTime, device, avg(cpu) FROM
   * metrics WHERE (device='d_1' OR device='d_2').
   */
  private StringBuilder getGroupByQuerySqlHead(List<DeviceSchema> devices, String aggFun,
      long timeUnit) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT time_bucket(").append(timeUnit).append(", time) AS sampleTime, device");

    addFunSensor(aggFun, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);
    addDeviceCondition(builder, devices);
    return builder;
  }

  /**
   * 创建查询语句--(不带有聚合函数的查询) .
   * SELECT time, device, cpu FROM metrics WHERE (device='d_1' OR device='d_2').
   */
  private StringBuilder getSampleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT time, device");
    addFunSensor(null, builder, devices.get(0).getSensors());

    builder.append(" FROM ").append(tableName);

    addDeviceCondition(builder, devices);
    return builder;
  }

  private void addFunSensor(String method, StringBuilder builder, List<Sensor> list) {
    if (method != null) {
      list.forEach(sensor ->
          builder.append(", ").append(method).append("(").append(sensor.getName()).append(")")
      );
    } else {
      list.forEach(sensor -> builder.append(", ").append(sensor.getName()));
    }
  }

  private void addDeviceCondition(StringBuilder builder, List<DeviceSchema> devices) {
    builder.append(" WHERE (");
    for (DeviceSchema deviceSchema : devices) {
      builder.append("device='").append(deviceSchema.getDevice()).append("'").append(" OR ");
    }
    builder.delete(builder.length() - 4, builder.length());
    builder.append(")");
  }

  /**
   * add time filter for query statements.
   *
   * @param builder sql header
   * @param rangeQuery range query
   */
  private static void addWhereTimeClause(StringBuilder builder, RangeQuery rangeQuery) {
    Timestamp startTimestamp = new Timestamp(rangeQuery.getStartTimestamp());
    Timestamp endTimestamp = new Timestamp(rangeQuery.getEndTimestamp());
    builder.append(" AND (time >= '").append(startTimestamp);
    builder.append("' AND time <= '").append(endTimestamp).append("') ");
  }

  /**
   * add value filter for query statements.
   *
   * @param devices query device schema
   * @param builder sql header
   * @param valueThreshold lower bound of query value filter
   */
  private static void addWhereValueClause(List<DeviceSchema> devices, StringBuilder builder,
      double valueThreshold) {
    boolean first = true;
    for (Sensor sensor : devices.get(0).getSensors()) {
      if (first) {
        builder.append(" AND (").append(sensor.getName()).append(" > ").append(valueThreshold);
        first = false;
      } else {
        builder.append(" and ").append(sensor.getName()).append(" > ").append(valueThreshold);
      }
    }
    builder.append(")");
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
            + "bike_id VARCHAR(20) REFERENCES bikes (bike_id), sensor_id VARCHAR(20) NOT NULL, value ")
            .append(sensorGroup.getDataType()).append(" NULL);");
    return sqlBuilder.toString();
  }

  private String getCreateBikesTableSql() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("CREATE TABLE bikes (bike_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, date_manufactured TIMESTAMPTZ);");
    return sqlBuilder.toString();
  }

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

  private String getConvertToHypertableSql(SensorGroup sensorGroup) {
    return String.format(CONVERT_TO_HYPERTABLE, sensorGroup.getTableName());
  }

  private List<String> getInsertOneBatchSingleTableSql(Batch batch) {
    //Map<Sensor, List<Point>> entries = batch.getEntries();
    // TODO fix
    return null;
  }

  private List<String> getInsertOneBatchSql(Batch batch) {
    Map<Sensor, Point[]> entries = batch.getEntries();
    DeviceSchema deviceSchema = batch.getDeviceSchema();
    List<String> sensorQueries = new ArrayList<>(deviceSchema.getSensors().size());
    StringBuilder sb = new StringBuilder();
    // FIXME different table names for different sensors
    for (Sensor sensor : deviceSchema.getSensors()) {
      if (entries.get(sensor).length == 0) {
        continue;
      }


      sb.append("insert into ").append(sensor.getSensorGroup().getTableName()).append(" (time, bike_id, sensor_id, value)")
              .append(" values ");

      boolean firstIteration = true;
      for (Point point : entries.get(sensor)) {
        if (firstIteration) {
          firstIteration = false;
        }
        else {
          sb.append(", ");
        }
        Timestamp timestamp = new Timestamp(point.getTimestamp());
        sb.append("('").append(timestamp).append("', ")
                .append("'").append(deviceSchema.getDevice()).append("', ")
                .append("'").append(sensor.getName()).append("', ")
                .append(point.getValue()).append(")");
      }
      sb.append(";");
      sensorQueries.add(sb.toString());
      sb.setLength(0);
    }

    return sensorQueries;
  }
}

