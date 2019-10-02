package de.uni_passau.dbts.benchmark.tsdb;

import de.uni_passau.dbts.benchmark.client.OperationController.Operation;
import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Measurement;
import de.uni_passau.dbts.benchmark.measurement.Status;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;

import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for database connections. It measures execution latencies and saves the them into a
 * {@link Measurement} instance.
 */
public class DBWrapper implements Database {

  private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

  /** Configuration singleton. */
  private static Config config = ConfigParser.INSTANCE.config();

  /** Database connection. */
  private Database db;

  /** Measurement results instance. */
  private Measurement measurement;

  /** Error log template. */
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";

  /**
   * Creates an wrapper instance.
   *
   * @param measurement Measurement results instance.
   */
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
        double timeInMillis = status.getCostTime() / Constants.NANO_TO_MILLIS;
        measureOperation(status, operation, batch.pointNum());
        String currentThread = Thread.currentThread().getName();
        double throughput = batch.pointNum() / (timeInMillis / Constants.MILLIS_TO_SECONDS);
        String log =
            String.format(
                Locale.US,
                "%s INSERT ONE batch latency (DEVICE: %s, GROUP: %s), %.2f ms, THROUGHPUT: %.2f points/s",
                currentThread,
                batch.getBike().getName(),
                batch.getBike().getGroup(),
                timeInMillis,
                throughput);
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
  public Status offlineBikes(Query query) {
    Status status = null;
    Operation operation = Operation.OFFLINE_BIKES;
    try {
      status = db.offlineBikes(query);
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
  public Status airPollutionHeatMap(Query query) {
    Status status = null;
    Operation operation = Operation.AIRQUALITY_HEATMAP;
    try {
      status = db.airPollutionHeatMap(query);
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
    try {
      if (config.CREATE_SCHEMA) {
        LOGGER.info("Registering schema...");
        st = System.nanoTime();
        db.registerSchema(schemaList);
        en = System.nanoTime();
        LOGGER.info("Successfully registered schema!");
      }
      createSchemaTimeInSecond = (en - st) / Constants.NANO_TO_SECONDS;
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

  /**
   * Adds a successfully executed operation to results.
   *
   * @param status Status of the execution.
   * @param operation Executed operation.
   * @param okPointNum Number of processed points.
   */
  private void measureOperation(Status status, Operation operation, int okPointNum) {
    measurement.addOkOperation(operation, status.getCostTime() / Constants.NANO_TO_MILLIS,
        okPointNum);
  }

  /**
   * Handles the results of an executed operation and logs the status.
   *
   * @param status Status of the execution.
   * @param operation Executed operation.
   */
  private void handleQueryOperation(Status status, Operation operation) {
    if (status.isOk()) {
      measureOperation(status, operation, status.getQueryResultPointNum());
      double timeInMillis = status.getCostTime() / Constants.NANO_TO_MILLIS;
      String formatTimeInMillis = String.format("%.2f", timeInMillis);
      String currentThread = Thread.currentThread().getName();
      LOGGER.info(
          "{} complete {} with latency: {} ms, {} result points",
          currentThread,
          operation,
          formatTimeInMillis,
          status.getQueryResultPointNum());
    } else {
      LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
      measurement.addFailOperation(operation);
      measurement.addOkPointNum(operation, status.getQueryResultPointNum());
    }
  }
}
