package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ClientMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.workload.IWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.slf4j.Logger;

/**
 * 负责人造数据的写入、查询，真实数据的查询。
 * 根据OPERATION_PROPORTION的比例执行写入和查询, 具体的查询和写入数据由workload确定。
 */
public abstract class BaseClient extends Client implements Runnable {

  protected static Logger LOGGER;

  private OperationController operationController;
  private IWorkload syntheticWorkload;
  private final SingletonWorkload singletonWorkload;
  private long insertLoopIndex;
  private DataSchema dataSchema = DataSchema.getInstance();
  private static ClientMonitoring clientMonitoring = ClientMonitoring.INSTANCE;


  public BaseClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier,
      IWorkload workload) {
    super(id, countDownLatch, barrier);
    syntheticWorkload = workload;
    singletonWorkload = SingletonWorkload.getInstance();
    operationController = new OperationController(id);
    insertLoopIndex = 0;
    initLogger();
  }

  void doTest() {
    for (long loopIndex = 0; loopIndex < config.LOOP; loopIndex++) {
      dbWrapper.incrementLoopIndex();
      Operation operation = operationController.getNextOperationType();
      switch (operation) {
        case INGESTION:
          clientMonitoring.start();
          if (config.BIND_CLIENTS_TO_DEVICES) {
            try {
              List<Bike> schema = dataSchema.getClientBindSchema().get(clientThreadId);
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
              dbWrapper.insertOneBatch(singletonWorkload.getOneBatch());
            } catch (Exception e) {
              LOGGER.error("Failed to insert one batch data because ", e);
            }
          }
          break;
        case PRECISE_POINT:
            clientMonitoring.start();
          try {
            dbWrapper.precisePoint(syntheticWorkload.getQuery("light"));
          } catch (Exception e) {
            LOGGER.error("Failed to do precise query because ", e);
          }
          break;
        case ACTIVE_BIKES:
          clientMonitoring.start();
          try {
            dbWrapper.lastTimeActivelyDriven(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do range query with value filter because ", e);
          }
          break;
        case DOWNSAMPLE:
            clientMonitoring.start();
          try {
            dbWrapper.downsample(syntheticWorkload.getQuery("oximeter"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do group by query because ", e);
          }
          break;
        case LAST_KNOWN_POSITION:
            clientMonitoring.start();
          try {
            dbWrapper.lastKnownPosition(syntheticWorkload.getQuery("emg"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute latest point query because ", e);
          }
          break;
        case GPS_PATH_SCAN:
            clientMonitoring.start();
          try {
            dbWrapper.gpsPathScan(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a time range query with GPS data because ", e);
          }
          break;
        case IDENTIFY_TRIPS:
            clientMonitoring.start();
          try {
            dbWrapper.identifyTrips(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a time range query on trip identification because ", e);
          }
          break;
        case AIRQUALITY_HEATMAP:
            clientMonitoring.start();
          try {
            dbWrapper.airPollutionHeatMap(syntheticWorkload.getQuery("particles"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a heat map range query because ", e);
          }
          break;
        case DISTANCE_DRIVEN:
          clientMonitoring.start();
          try {
            dbWrapper.distanceDriven(syntheticWorkload.getQuery("current"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a distance range query because ", e);
          }
          break;
        case BIKES_IN_LOCATION:
          clientMonitoring.start();
          try {
            dbWrapper.bikesInLocation(syntheticWorkload.getQuery("emg"));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute the bikes in location query because ", e);
          }
          break;
        case OFFLINE_BIKES:
          clientMonitoring.start();
          try {
            dbWrapper.offlineBikes(syntheticWorkload.getQuery(""));
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute the gps aggregation in range query because ", e);
          }
          break;
        default:
          LOGGER.error("Unsupported operation type {}", operation);
      }

      String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.LOOP);
      LOGGER.info("{} {}% syntheticWorkload is done.", Thread.currentThread().getName(), percent);
    }
  }

  abstract void initLogger();

}

