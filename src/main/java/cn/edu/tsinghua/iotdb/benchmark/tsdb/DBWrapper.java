package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.Query;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;

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
                currentThread, batch.getBike().getName(),
                batch.getBike().getGroup(), timeInMillis, throughput);
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

  @Override
  public Status precisePoint(Query query) {
    Status status = null;
    Operation operation = Operation.PRECISE_POINT;
    try {
      status = db.precisePoint(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status gpsPathScan(Query query) {
    Status status = null;
    Operation operation = Operation.GPS_PATH_SCAN;
    try {
      status = db.gpsPathScan(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status identifyTrips(Query query) {
    Status status = null;
    Operation operation = Operation.IDENTIFY_TRIPS;
    try {
      status = db.identifyTrips(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status trafficJams(Query query) {
    Status status = null;
    Operation operation = Operation.TRAFFIC_JAMS;
    try {
      status = db.trafficJams(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status lastTimeActivelyDriven(Query query) {
    Status status = null;
    Operation operation = Operation.ACTIVE_BIKES;
    try {
      status = db.lastTimeActivelyDriven(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status downsample(Query query) {
    Status status = null;
    Operation operation = Operation.DOWNSAMPLE;
    try {
      status = db.downsample(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status lastKnownPosition(Query query) {
    Status status = null;
    Operation operation = Operation.LAST_KNOWN_POSITION;
    try {
      status = db.lastKnownPosition(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status airQualityHeatMap(Query query) {
    Status status = null;
    Operation operation = Operation.AIRQUALITY_HEATMAP;
    try {
      status = db.airQualityHeatMap(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status distanceDriven(Query query) {
    Status status = null;
    Operation operation = Operation.DISTANCE_DRIVEN;
    try {
      status = db.distanceDriven(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  @Override
  public Status bikesInLocation(Query query) {
    Status status = null;
    Operation operation = Operation.BIKES_IN_LOCATION;
    try {
      status = db.bikesInLocation(query);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      measurement.addFailOperation(operation);
      LOGGER.error(ERROR_LOG, operation, e);
    }
    return status;
  }

  public void incrementLoopIndex() {
    measurement.incrementLoopIndex();
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
  public void registerSchema(List<Bike> schemaList) throws TsdbException {
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
