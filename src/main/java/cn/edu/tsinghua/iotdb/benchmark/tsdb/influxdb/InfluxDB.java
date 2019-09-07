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

import java.util.*;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import org.influxdb.BatchOptions;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDB implements IDatabase {
  public static final int MILLIS_TO_NANO = 1000000;

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDB.class);
  private static Config config = ConfigParser.INSTANCE.config();

  private final String influxUrl;
  private final String influxDbName;
  private final String defaultRp = "autogen";
  private final String dataType;
  private final String measurementName = "test";

  private org.influxdb.InfluxDB influxDbInstance;
  private SqlBuilder sqlBuilder;

  /**
   * Creates an instance of the InfluxDB controller.
   */
  public InfluxDB() {
    influxUrl = String.format("http://%s:%s", config.HOST, config.PORT);
    influxDbName = config.DB_NAME;
    dataType = config.DATA_TYPE.toLowerCase();
    sqlBuilder = new InfluxBuilder();
  }

  @Override
  public void init() throws TsdbException {
    try {
      influxDbInstance = org.influxdb.InfluxDBFactory.connect(influxUrl)
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
    Map<Sensor, Point[]> entries = batch.getEntries();
    Bike bike = batch.getBike();
    for (Sensor sensor : bike.getSensors()) {
      if (entries.get(sensor).length == 0) {
        continue;
      }

      for (Point syntheticPoint : entries.get(sensor)) {
        org.influxdb.dto.Point.Builder pointBuilder = org.influxdb.dto.Point.measurement(measurementName)
                .tag(SqlBuilder.Column.BIKE.getName(), bike.getName())
                .tag(SqlBuilder.Column.SENSOR_GROUP.getName(), sensor.getSensorGroup().getName())
                .tag(SqlBuilder.Column.SENSOR.getName(), sensor.getName());

        List<String> fields = sensor.getFields();
        if (fields.size() > 1) {
          String[] values = syntheticPoint.getValues();
          for (int i = 0; i < fields.size(); i++) {
            pointBuilder.addField(fields.get(i), Double.parseDouble(values[i]));
          }
        } else {
          double value = Double.parseDouble(syntheticPoint.getValue());
          pointBuilder.addField("value", value);
        }
        org.influxdb.dto.Point influxPoint = pointBuilder.time(syntheticPoint.getTimestamp(), TimeUnit.MILLISECONDS)
                .build();
        batchBuilder.point(influxPoint);
      }
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

  @Override
  public Status precisePoint(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status gpsPathScan(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status identifyTrips(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status offlineBikes(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status lastTimeActivelyDriven(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status downsample(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status lastKnownPosition(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
  }

  @Override
  public Status airPollutionHeatMap(cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query query) {
    return null;
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

  /**
   * generate from and where clause for specified devices.
   *
   * @param devices schema list of query devices
   * @return from and where clause
   */
  private static String generateConstrainForDevices(List<Bike> devices) {
    StringBuilder builder = new StringBuilder();
    Set<String> groups = new HashSet<>();
    for (Bike d : devices) {
      groups.add(d.getGroup());
    }
    builder.append(" FROM ");
    for (String g : groups) {
      builder.append(g).append(" , ");
    }
    builder.deleteCharAt(builder.lastIndexOf(","));
    builder.append("WHERE (");
    for (Bike d : devices) {
      builder.append(" device = '" + d.getName() + "' OR");
    }
    builder.delete(builder.lastIndexOf("OR"), builder.length());
    builder.append(")");

    return builder.toString();
  }

}