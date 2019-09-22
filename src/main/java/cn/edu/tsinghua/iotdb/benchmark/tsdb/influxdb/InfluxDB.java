package cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.InfluxBuilder;
import cn.edu.tsinghua.iotdb.benchmark.utils.SqlBuilder;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Point;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import okhttp3.OkHttpClient;
import org.influxdb.BatchOptions;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilder;

public class InfluxDB implements IDatabase {
  public static final long MILLIS_TO_NANO = 1000000L;

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigParser.INSTANCE.config();

  private final String influxUrl;
  private final String influxDbName;
  private final String defaultRp = "autogen";
  private final String dataType;
  private final String measurementName = "test";

  private org.influxdb.InfluxDB influxDbInstance;
  private SqlBuilder sqlBuilder;

  /** Creates an instance of the InfluxDB controller. */
  public InfluxDB() {
    influxUrl = String.format("http://%s:%s", config.HOST, config.PORT);
    influxDbName = config.DB_NAME;
    dataType = config.DATA_TYPE.toLowerCase();
    sqlBuilder = new InfluxBuilder();
  }

  @Override
  public void init() throws TsdbException {
    try {
      OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
      clientBuilder
          .connectTimeout(20, TimeUnit.MINUTES)
          .readTimeout(20, TimeUnit.MINUTES)
          .writeTimeout(20, TimeUnit.MINUTES)
          .retryOnConnectionFailure(true);
      influxDbInstance =
          org.influxdb.InfluxDBFactory.connect(influxUrl, clientBuilder)
              .setDatabase(influxDbName)
              .setRetentionPolicy(defaultRp)
              .setConsistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL)
              .enableBatch(BatchOptions.DEFAULTS.jitterDuration(500));
    } catch (Exception e) {
      LOGGER.error("Initialize InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      if (influxDbInstance.databaseExists(influxDbName)) {
        influxDbInstance.query(new Query("DROP DATABASE " + influxDbName));
      }

      LOGGER.info("Waiting {}ms for old data deletion.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (Exception e) {
      LOGGER.error("Cleanup InfluxDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() {
    if (influxDbInstance != null) {
      influxDbInstance.close();
    }
  }

  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try {
      influxDbInstance.query(new Query("CREATE DATABASE " + influxDbName));
    } catch (Exception e) {
      LOGGER.error("RegisterSchema InfluxDB failed because: ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    BatchPoints.Builder batchBuilder = BatchPoints.builder().precision(TimeUnit.MILLISECONDS);
    Bike bike = batch.getBike();

    Map<Long, Map<String, String>> rows = transformBatch(batch);

    for (long timestamp : rows.keySet()) {
      org.influxdb.dto.Point.Builder pointBuilder =
          org.influxdb.dto.Point.measurement(measurementName)
              .tag(SqlBuilder.Column.BIKE.getName(), bike.getName())
              .tag(SqlBuilder.Column.OWNER_NAME.getName(), bike.getOwnerName());

      Map<String, String> fields = rows.get(timestamp);
      for (String field : fields.keySet()) {
        pointBuilder.addField(field, Double.parseDouble(fields.get(field)));
      }
      org.influxdb.dto.Point influxPoint =
          pointBuilder.time(timestamp, TimeUnit.MILLISECONDS).build();
      batchBuilder.point(influxPoint);
    }
    BatchPoints batchPoints = batchBuilder.build();

    try {
      long startTime = System.nanoTime();
      influxDbInstance.write(batchPoints);
      long endTime = System.nanoTime();
      long latency = endTime - startTime;
      return new Status(true, latency);
    } catch (Exception e) {
      LOGGER.error("Could not insert batch because: {}", e.getMessage());
      return new Status(false, 0, e, e.toString());
    }
  }

  private long trailingZeros(long timestamp) {
    return timestamp * MILLIS_TO_NANO;
  }

  /**
   * <code>
   *     SELECT * FROM test WHERE bike_id = 'bike_2' AND time = 1535587200000000000;
   * </code>
   *
   * @param query universal precise query condition parameters
   * @return
   */
  @Override
  public Status precisePoint(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    long timestamp = trailingZeros(query.getStartTimestamp());
    Bike bike = query.getBikes().get(0);

    String sql = "SELECT * FROM %s WHERE bike_id = '%s' AND time = %d;";
    sql = String.format(Locale.US, sql, measurementName, bike.getName(), timestamp);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * <code>
   *     SELECT longitude, latitude FROM test
   *     WHERE bike_id = 'bike_0' AND time > 1535587200000000000 and time < 1535590800000000000;
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status gpsPathScan(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    Bike bike = query.getBikes().get(0);
    long startTimestamp = trailingZeros(query.getStartTimestamp());
    long endTimestamp = trailingZeros(query.getEndTimestamp());
    String sql =
        "SELECT longitude, latitude FROM %s WHERE bike_id = '%s' AND time > %d and time < %d;";
    sql =
        String.format(
            Locale.US, sql, measurementName, bike.getName(), startTimestamp, endTimestamp);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * <code>
   *     SELECT * FROM (
   *        SELECT MEAN(s_17) AS avg, FIRST(longitude) as longitude, LAST(latitude) AS latitude
   *        FROM test WHERE bike_id = 'bike_2' AND time >= 1535587200000000000 AND time <= 1535590800000000000
   *        GROUP BY time(1s)
   *     ) WHERE avg > 1000.000000;
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status identifyTrips(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    Sensor sensor = query.getSensor();
    Bike bike = query.getBikes().get(0);
    long startTimestamp = trailingZeros(query.getStartTimestamp());
    long endTimestamp = trailingZeros(query.getEndTimestamp());
    String sql =
        "SELECT * FROM(SELECT MEAN(%s) AS avg, FIRST(longitude) as longitude, LAST(latitude) AS latitude "
            + "FROM %s WHERE bike_id = '%s' AND time >= %d AND time <= %d GROUP BY time(1s)) WHERE avg > %f;";
    sql =
        String.format(
            Locale.US,
            sql,
            sensor.getName(),
            measurementName,
            bike.getName(),
            startTimestamp,
            endTimestamp,
            query.getThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * <code>
   *     SELECT bike_id FROM (SELECT LAST(s_0) FROM test GROUP BY bike_id) WHERE time < 1535590800000000000;
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status offlineBikes(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    long timestamp = trailingZeros(query.getEndTimestamp());
    Sensor sensor = query.getSensor();
    String sql = "SELECT bike_id FROM (SELECT LAST(%s) FROM %s GROUP BY bike_id) WHERE time < %d;";
    sql = String.format(Locale.US, sql, sensor.getName(), measurementName, timestamp);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * <code>
   *     SELECT LAST("avg") FROM (SELECT MEAN(s_17) AS avg FROM test WHERE time > 1535587200000000000
   *     GROUP BY time(1m), bike_id, owner_name) WHERE "avg" > 3.000000 GROUP BY bike_id, owner_name;
   * </code>
   *
   * @param query contains universal range query with value filter parameters
   * @return
   */
  @Override
  public Status lastTimeActivelyDriven(
      cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    long timestamp = trailingZeros(query.getStartTimestamp());
    Sensor sensor = query.getSensor();
    String sql =
        "SELECT LAST(\"avg\") FROM (SELECT MEAN(%s) AS avg FROM %s WHERE time > %d "
            + "GROUP BY time(1m), bike_id, owner_name) WHERE \"avg\" > %f GROUP BY bike_id, owner_name;";
    sql =
        String.format(
            Locale.US, sql, sensor.getName(), measurementName, timestamp, query.getThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * <code>
   *     SELECT MEAN(s_27) FROM test
   *     WHERE bike_id = 'bike_2' AND time > 1535587200000000000 AND time < 1535590800000000000
   *     GROUP BY time(1m) fill(none);
   * </code>
   *
   * @param query contains universal group by query condition parameters
   * @return
   */
  @Override
  public Status downsample(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    Bike bike = query.getBikes().get(0);
    Sensor sensor = query.getSensor();
    long startTimestamp = trailingZeros(query.getStartTimestamp());
    long endTimestamp = trailingZeros(query.getEndTimestamp());

    String sql =
        "SELECT MEAN(%s) FROM %s WHERE bike_id = '%s' AND time > %d AND time < %d GROUP BY time(1m) fill(none);";
    sql =
        String.format(
            Locale.US,
            sql,
            sensor.getName(),
            measurementName,
            bike.getName(),
            startTimestamp,
            endTimestamp);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status lastKnownPosition(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    String sql =
        "SELECT LAST(longitude) AS longitude, LAST(latitude) AS latitude FROM %s "
            + "GROUP BY bike_id, bike_owner;";
    sql = String.format(Locale.US, sql, measurementName);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * *
   *
   * <p><code>
   *     SELECT FIRST(longitude), FIRST(latitude), MEAN(s_34) FROM test
   *     WHERE time >= 1535587200000000000 and time <= 1535590800000000000
   *     GROUP BY time(1s);
   * </code>
   *
   * @param query
   * @return
   */
  @Override
  public Status airPollutionHeatMap(
      cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    long startTimestamp = trailingZeros(query.getStartTimestamp());
    long endTimestamp = trailingZeros(query.getEndTimestamp());
    Sensor sensor = query.getSensor();
    String sql =
        "SELECT FIRST(longitude), FIRST(latitude), MEAN(%s) FROM %s WHERE time >= %d and time <= %d "
            + "GROUP BY time(1s);";
    sql =
        String.format(
            Locale.US, sql, sensor.getName(), measurementName, startTimestamp, endTimestamp);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status distanceDriven(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status bikesInLocation(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.debug("{} executes query: {}", Thread.currentThread().getName(), sql);
    long startTimeStamp = System.nanoTime();
    QueryResult results = influxDbInstance.query(new Query(sql, influxDbName));
    int cnt = 0;
    for (Result result : results.getResults()) {
      List<Series> series = result.getSeries();
      if (series == null) {
        continue;
      }
      if (result.getError() != null) {
        return new Status(false, 0, cnt, new Exception(result.getError()), sql);
      }
      for (Series serie : series) {
        List<List<Object>> values = serie.getValues();
        cnt += values.size() * (serie.getColumns().size() - 1);
      }
    }
    long endTimeStamp = System.nanoTime();
    LOGGER.debug("{} got result set size: {}", Thread.currentThread().getName(), cnt);
    return new Status(true, endTimeStamp - startTimeStamp, cnt);
  }

  private Map<Long, Map<String, String>> transformBatch(Batch batch) {
    Map<Long, Map<String, String>> rows = new HashMap<>();
    Bike bike = batch.getBike();
    Map<Sensor, Point[]> entries = batch.getEntries();
    for (Sensor sensor : bike.getSensors()) {
      Point[] points = entries.get(sensor);
      for (Point point : points) {
        rows.computeIfAbsent(point.getTimestamp(), k -> new HashMap<>());
        if (point.hasMultipleValues()) {
          List<String> fields = sensor.getFields();
          String[] values = point.getValues();
          for (int i = 0; i < fields.size(); i++) {
            rows.get(point.getTimestamp()).put(fields.get(i), values[i]);
          }
        } else {
          rows.get(point.getTimestamp()).put(sensor.getName(), point.getValue());
        }
      }
    }
    return rows;
  }
}
