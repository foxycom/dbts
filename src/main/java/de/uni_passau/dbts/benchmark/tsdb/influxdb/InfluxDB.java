package de.uni_passau.dbts.benchmark.tsdb.influxdb;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.tsdb.Database;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.utils.SqlBuilder;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.ingestion.Point;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.NotImplementedException;
import org.influxdb.BatchOptions;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of benchmark scenarios for InfluxDB.
 */
public class InfluxDB implements Database {
  private static final long MILLIS_TO_NANO = 1000000L;

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigParser.INSTANCE.config();

  /** InfluxDB's API URL. */
  private final String url;

  /** Name of the database to use. */
  private final String dbName;

  /**
   * Retention policy to use for data.
   * Autogen stores data for an indefinite period of time.
   */
  private final String defaultRp = "autogen";

  /** Name of the measurement to use. Measurements are "similar" to tables. */
  private final String measurementName = "test";

  /** DB instance. */
  private org.influxdb.InfluxDB influxDbInstance;

  /** Creates an instance of the InfluxDB controller. */
  public InfluxDB() {
    url = String.format("http://%s:%s", config.HOST, config.PORT);
    dbName = config.DB_NAME;
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
          org.influxdb.InfluxDBFactory.connect(url, clientBuilder)
              .setDatabase(dbName)
              .setRetentionPolicy(defaultRp)
              .setConsistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL)
              .enableBatch(BatchOptions.DEFAULTS.jitterDuration(500));
    } catch (Exception e) {
      LOGGER.error("InfluxDB could not be initialized because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      if (influxDbInstance.databaseExists(dbName)) {
        influxDbInstance.query(new Query("DROP DATABASE " + dbName));
      }

      LOGGER.info("Waiting {}ms until old data has been erased.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (Exception e) {
      LOGGER.error("InfluxDB data could not be erased because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() {
    if (influxDbInstance != null) {
      influxDbInstance.close();
    }
  }

  /**
   * Creates a new database. InfluxDB is schema-less; thus, no further steps must be done.
   *
   * @param schemaList Schema of devices to be registered.
   * @throws TsdbException if an error occurred while creating InfluxDB database.
   */
  @Override
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
    try {
      influxDbInstance.query(new Query("CREATE DATABASE " + dbName));
    } catch (Exception e) {
      LOGGER.error("InfluxDB database could not be created because: ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }

  /**
   * {@inheritDoc} Creates a tag set {bike_id, owner_name} and a field set with each sensor value
   * being a single field. InfluxDB does not provide support for geospatial data types; thus, a GPS
   * sensor yields 2 values that are two separate fields.
   */
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

  /**
   * Converts a milliseconds timestamp to a nano seconds timestamp.
   *
   * @param timestamp Milliseconds timestamp.
   * @return Nano seconds timestamp.
   */
  private long trailingZeros(long timestamp) {
    return timestamp * MILLIS_TO_NANO;
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT * FROM test WHERE bike_id = 'bike_2' AND time = 1535587200000000000;
   * </code>
   */
  @Override
  public Status precisePoint(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    long timestamp = trailingZeros(query.getStartTimestamp());
    Bike bike = query.getBikes().get(0);

    String sql = "SELECT * FROM %s WHERE bike_id = '%s' AND time = %d;";
    sql = String.format(Locale.US, sql, measurementName, bike.getName(), timestamp);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT longitude, latitude FROM test
   *     WHERE bike_id = 'bike_0' AND time &gt; 1535587200000000000 and time &lt; 1535590800000000000;
   * </code>
   */
  @Override
  public Status gpsPathScan(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT * FROM (
   *        SELECT MEAN(s_17) AS avg, FIRST(longitude) as longitude, LAST(latitude) AS latitude
   *        FROM test WHERE bike_id = 'bike_2' AND time &gt;= 1535587200000000000 AND time &lt;= 1535590800000000000
   *        GROUP BY time(1s)
   *     ) WHERE avg &gt; 1000.000000;
   * </code>
   */
  @Override
  public Status identifyTrips(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT bike_id FROM (SELECT LAST(s_0) FROM test GROUP BY bike_id) WHERE time &lt; 1535590800000000000;
   * </code>
   */
  @Override
  public Status offlineBikes(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    long timestamp = trailingZeros(query.getEndTimestamp());
    Sensor sensor = query.getSensor();
    String sql = "SELECT bike_id FROM (SELECT LAST(%s) FROM %s GROUP BY bike_id) WHERE time < %d;";
    sql = String.format(Locale.US, sql, sensor.getName(), measurementName, timestamp);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT LAST("avg") FROM (SELECT MEAN(s_17) AS avg FROM test WHERE time &gt; 1535587200000000000
   *     GROUP BY time(1m), bike_id, owner_name) WHERE "avg" &gt; 3.000000 GROUP BY bike_id, owner_name;
   * </code>
   */
  @Override
  public Status lastTimeActivelyDriven(
      de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
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
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT MEAN(s_27) FROM test
   *     WHERE bike_id = 'bike_2' AND time &gt; 1535587200000000000 AND time &lt; 1535590800000000000
   *     GROUP BY time(1m) fill(none);
   * </code>
   */
  @Override
  public Status downsample(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
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

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *    SELECT LAST(longitude) AS longitude, LAST(latitude) AS latitude FROM test
   *    GROUP BY bike_id, bike_owner;
   * </code>
   */
  @Override
  public Status lastKnownPosition(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    String sql =
        "SELECT LAST(longitude) AS longitude, LAST(latitude) AS latitude FROM %s "
            + "GROUP BY bike_id, bike_owner;";
    sql = String.format(Locale.US, sql, measurementName);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * {@inheritDoc} Example query:
   *
   * <p><code>
   *     SELECT FIRST(longitude), FIRST(latitude), MEAN(s_34) FROM test
   *     WHERE time &gt;= 1535587200000000000 and time &lt;= 1535590800000000000
   *     GROUP BY time(1s);
   * </code>
   */
  @Override
  public Status airPollutionHeatMap(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
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

  /**
   * InfluxDB does not support geospatial distance aggregations.
   *
   * @param query Query params object.
   * @return Status of the aggregation.
   */
  @Override
  public Status distanceDriven(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return new Status(
        false,
        0,
        new NotImplementedException(""),
        "InfluxDB does not support distance aggregations.");
  }

  /**
   * TODO update
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  @Override
  public Status bikesInLocation(de.uni_passau.dbts.benchmark.workload.query.impl.Query query) {
    return null;
  }

  /**
   * Executes the InfluxQL query and returns status of the execution.
   *
   * @param sql SQL (InfluxQL) query.
   * @return Status of the execution.
   */
  private Status executeQueryAndGetStatus(String sql) {
    LOGGER.debug("{} executes query: {}", Thread.currentThread().getName(), sql);
    long startTimeStamp = System.nanoTime();
    QueryResult results = influxDbInstance.query(new Query(sql, dbName));
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

  /**
   * Transforms a batch into InfluxDB friendly format. Keys are timestamps which point to fields
   * names with appropriate values.
   *
   * @param batch Batch of points.
   * @return InfluxDB friendly batch of points.
   */
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
