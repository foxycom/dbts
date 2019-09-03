package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBWrapper implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IDatabase.class);
  private static Config config = ConfigParser.INSTANCE.config();
  private IDatabase db;
  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private Measurement measurement;
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";


  public DBWrapper(Measurement measurement) {
    DBFactory dbFactory = new DBFactory();
    try {
      db = dbFactory.getDatabase();
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
    }
    this.measurement = measurement;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {
      status = db.insertOneBatch(batch);
      if (status.isOk()) {
        double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
        measureOperation(status, operation, batch.pointNum());
        String currentThread = Thread.currentThread().getName();
        double throughput = batch.pointNum() / (timeInMillis / Constants.MILLIS_TO_SECONDS);
        String log = String.format(Locale.US, "%s INSERT ONE batch latency (DEVICE: %s, GROUP: %s), %.2f ms, THROUGHPUT: %.2f points/s",
                currentThread, batch.getDeviceSchema().getDevice(),
                batch.getDeviceSchema().getGroup(), timeInMillis, throughput);
        LOGGER.info(log);
      } else {
        measurement.addFailOperation(operation, batch.pointNum());
      }
    } catch (Exception e) {
      measurement.addFailOperation(operation, batch.pointNum());
      LOGGER.error("Failed to insert one batch because ofç unexpected exception: ", e);
    }
    return status;
  }

  public void incrementLoopIndex() {
    measurement.incrementLoopIndex();
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    Status status = null;
    Operation operation = Operation.PRECISE_QUERY;
    try {
      status = db.preciseQuery(preciseQuery);
      handleQueryOperation(status, operation);

    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY;
    try {
      status = db.rangeQuery(rangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status gpsRangeQuery(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.GPS_TIME_RANGE_QUERY;
    try {
      status = db.gpsRangeQuery(rangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status gpsValueRangeQuery(ValueRangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.GPS_TRIP_RANGE_QUERY;
    try {
      status = db.gpsValueRangeQuery(rangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }


  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    Status status = null;
    Operation operation = Operation.VALUE_RANGE_QUERY;
    try {
      status = db.valueRangeQuery(valueRangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_QUERY;
    try {
      status = db.aggRangeQuery(aggRangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    Status status = null;
    Operation operation = Operation.AGG_VALUE_QUERY;
    try {
      status = db.aggValueQuery(aggValueQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_VALUE_QUERY;
    try {
      status = db.aggRangeValueQuery(aggRangeValueQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    Status status = null;
    Operation operation = Operation.GROUP_BY_QUERY;
    try {
      status = db.groupByQuery(groupByQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    Status status = null;
    Operation operation = Operation.LATEST_POINT_QUERY;
    try {
      status = db.latestPointQuery(latestPointQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status heatmapRangeQuery(HeatmapRangeQuery heatmapRangeQuery) {
    Status status = null;
    Operation operation = Operation.HEATMAP_RANGE_QUERY;
    try {
      status = db.heatmapRangeQuery(heatmapRangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status distanceRangeQuery(ValueRangeQuery valueRangeQuery) {
    Status status = null;
    Operation operation = Operation.DISTANCE_RANGE_QUERY;
    try {
      status = db.distanceRangeQuery(valueRangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status bikesInLocationQuery(HeatmapRangeQuery heatmapRangeQuery) {
    Status status = null;
    Operation operation = Operation.BIKES_IN_LOCATION_QUERY;
    try {
      status = db.bikesInLocationQuery(heatmapRangeQuery);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public void init() throws TsdbException {
    db.init();
  }

  @Override
  public void cleanup() throws TsdbException {
    db.cleanup();
  }

  @Override
  public void close() throws TsdbException {
    db.close();
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    double createSchemaTimeInSecond;
    long en = 0;
    long st = 0;
    LOGGER.info("Registering schema...");
    try {
      if (config.CREATE_SCHEMA) {
        st = System.nanoTime();
        db.registerSchema(schemaList);
        en = System.nanoTime();
        LOGGER.info("Successfully registered schema!");
      }
      createSchemaTimeInSecond = (en - st) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaTime(0);
      System.out.println();
      throw new TsdbException(e);
    }
  }

  @Override
  public float getSize() throws TsdbException {
    return db.getSize();
  }

  private void measureOperation(Status status, Operation operation, int okPointNum) {
    measurement.addOkOperation(operation, status.getCostTime() / NANO_TO_MILLIS, okPointNum);
  }

  private void handleQueryOperation(Status status, Operation operation){
    if (status.isOk()) {
      measureOperation(status, operation, status.getQueryResultPointNum());
      double timeInMillis = status.getCostTime() / NANO_TO_MILLIS;
      String formatTimeInMillis = String.format("%.2f", timeInMillis);
      String currentThread = Thread.currentThread().getName();
      LOGGER
          .info("{} complete {} with latency: {} ms, {} result points", currentThread, operation,
              formatTimeInMillis, status.getQueryResultPointNum());
    } else {
      LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
      measurement.addFailOperation(operation);
      // currently we do not have expected result point number
      measurement.addOkPointNum(operation, status.getQueryResultPointNum());
    }
  }

}
