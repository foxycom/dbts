package de.uni_passau.dbts.benchmark.client;

import de.uni_passau.dbts.benchmark.client.OperationController.Operation;
import de.uni_passau.dbts.benchmark.monitor.MonitoringClient;
import de.uni_passau.dbts.benchmark.workload.Workload;
import de.uni_passau.dbts.benchmark.workload.WorkloadException;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.BikesSchema;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;

/**
 * This class implements the logic of benchmarks with synthetic data. Each client carries a
 * workload instance, which generates batches of synthetic data or query parameters. The client
 * forwards the generated workload to the database wrapper.
 */
public abstract class BaseClient extends Client implements Runnable {

  protected static Logger LOGGER;

  /** Yields operations to execute. */
  private OperationController operationController;

  /** Workload instance. */
  private Workload syntheticWorkload;

  /** Helper variable to track the progress of ingestion. */
  private long insertLoopIndex;

  /** Schema of bikes the client has to process. */
  private BikesSchema bikesSchema = BikesSchema.getInstance();

  /**
   * Singleton of a system monitor client, which the worker thread can tell to start or stop monitoring
   * system stats to.
   */
  private static MonitoringClient monitoringClient = MonitoringClient.INSTANCE;

  /**
   * Creates an instance of a worker client.
   *
   * @param id The client's ID.
   * @param countDownLatch Down latch object.
   * @param barrier A barrier the client should be bound to.
   * @param workload Workload instance.
   */
  public BaseClient(
      int id, CountDownLatch countDownLatch, CyclicBarrier barrier, Workload workload) {
    super(id, countDownLatch, barrier);
    syntheticWorkload = workload;
    operationController = new OperationController(id);
    insertLoopIndex = 0;
    initLogger();
  }

  /**
   * Starts execution of operations. Within each loop tick, the next operation to execute is chosen
   * based on the configured probabilities.
   */
  void doTest() {
    for (long loopIndex = 0; loopIndex < config.LOOP; loopIndex++) {
      dbWrapper.incrementLoopIndex();
      Operation operation = operationController.getNextOperationType();
      switch (operation) {
        case INGESTION:
          monitoringClient.start();
          if (config.BIND_CLIENTS_TO_DEVICES) {
            try {
              List<Bike> schema = bikesSchema.getClientBindSchema().get(clientThreadId);
              int i = 1;
              for (Bike bike : schema) {
                Batch batch = syntheticWorkload.getOneBatch(bike, insertLoopIndex);
                barrier.await();
                dbWrapper.insertOneBatch(batch);
                double progress = (double) (i * 100) / schema.size();
                LOGGER.info("Progress within loop: {}%", progress);
                i++;
              }
            } catch (Exception e) {
              LOGGER.error("Failed to insert one batch data because ", e);
            }
            insertLoopIndex++;
          } else {
            try {
              // dbWrapper.insertOneBatch(syntheticWorkload.getOneBatch());
              throw new NotImplementedException("");
            } catch (Exception e) {
              LOGGER.error("Failed to insert one batch data because ", e);
            }
          }
          break;
        case PRECISE_POINT:
          monitoringClient.start();
          try {
            dbWrapper.precisePoint(syntheticWorkload.getQuery("light"));
          } catch (Exception e) {
            LOGGER.error("Failed to do precise query because ", e);
          }
          break;
        case ACTIVE_BIKES:
          monitoringClient.start();
          try {
            dbWrapper.lastTimeActivelyDriven(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to retrieve last trips of each bike because ", e);
          }
          break;
        case DOWNSAMPLE:
          monitoringClient.start();
          try {
            dbWrapper.downsample(syntheticWorkload.getQuery("oximeter"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to downsample sensors metrics because ", e);
          }
          break;
        case LAST_KNOWN_POSITION:
          monitoringClient.start();
          try {
            dbWrapper.lastKnownPosition(syntheticWorkload.getQuery("emg"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to retrieve last known positions of each bike because ", e);
          }
          break;
        case GPS_PATH_SCAN:
          monitoringClient.start();
          try {
            dbWrapper.gpsPathScan(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to scan GPS path of a bike because ", e);
          }
          break;
        case IDENTIFY_TRIPS:
          monitoringClient.start();
          try {
            dbWrapper.identifyTrips(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to identify trips because ", e);
          }
          break;
        case AIRQUALITY_HEATMAP:
          monitoringClient.start();
          try {
            dbWrapper.airPollutionHeatMap(syntheticWorkload.getQuery("particles"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to build a heatmap because ", e);
          }
          break;
        case DISTANCE_DRIVEN:
          monitoringClient.start();
          try {
            dbWrapper.distanceDriven(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to compute the distance a bike has driven because ", e);
          }
          break;
        case BIKES_IN_LOCATION:
          monitoringClient.start();
          try {
            dbWrapper.bikesInLocation(syntheticWorkload.getQuery("emg"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to find bikes in a specific area because ", e);
          }
          break;
        case OFFLINE_BIKES:
          monitoringClient.start();
          try {
            dbWrapper.offlineBikes(syntheticWorkload.getQuery(""));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to retrieve offline bikes because ", e);
          }
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }

      String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.LOOP);
      LOGGER.info(
          "{} {}% of synthetic workload is done.", Thread.currentThread().getName(), percent);
    }
  }

  /**
   * Initializes a logger.
   */
  abstract void initLogger();
}
