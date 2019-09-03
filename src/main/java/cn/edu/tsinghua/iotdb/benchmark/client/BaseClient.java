package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ClientMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.workload.IWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
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
          if (config.BIND_CLIENTS_TO_DEVICES) {
            try {
              List<DeviceSchema> schema = dataSchema.getClientBindSchema().get(clientThreadId);
              int i = 1;
              for (DeviceSchema deviceSchema : schema) {
                barrier.await();
                Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
                clientMonitoring.start();
                dbWrapper.insertOneBatch(batch);
                clientMonitoring.stop();
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
        case PRECISE_QUERY:
            clientMonitoring.start();
          try {
            dbWrapper.preciseQuery(syntheticWorkload.getPreciseQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do precise query because ", e);
          }
          break;
        case RANGE_QUERY:
          clientMonitoring.start();
          try {
            dbWrapper.rangeQuery(syntheticWorkload.getRangeQuery());
          } catch (Exception e) {
            LOGGER.error("Failed to do range query because ", e);
          }
          break;
        case VALUE_RANGE_QUERY:
          clientMonitoring.start();
          try {
            dbWrapper.valueRangeQuery(syntheticWorkload.getValueRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do range query with value filter because ", e);
          }
          break;
        case AGG_RANGE_QUERY:
          clientMonitoring.start();
          try {
            dbWrapper.aggRangeQuery(syntheticWorkload.getAggRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation range query because ", e);
          }
          break;
        case AGG_VALUE_QUERY:
          clientMonitoring.start();
          try {
            dbWrapper.aggValueQuery(syntheticWorkload.getAggValueQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation query with value filter because ", e);
          }
          break;
        case AGG_RANGE_VALUE_QUERY:
          clientMonitoring.start();
          try {
            dbWrapper.aggRangeValueQuery(syntheticWorkload.getAggRangeValueQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do aggregation range query with value filter because ", e);
          }
          break;
        case GROUP_BY_QUERY:
            clientMonitoring.start();
          try {
            dbWrapper.groupByQuery(syntheticWorkload.getGroupByQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to do group by query because ", e);
          }
          break;
        case LATEST_POINT_QUERY:
            clientMonitoring.start();
          try {
            dbWrapper.latestPointQuery(syntheticWorkload.getLatestPointQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute latest point query because ", e);
          }
          break;

        case GPS_TIME_RANGE_QUERY:
            clientMonitoring.start();
          try {
            dbWrapper.gpsRangeQuery(syntheticWorkload.getGpsRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a time range query with GPS data because ", e);
          }
          break;
        case GPS_TRIP_RANGE_QUERY:
            clientMonitoring.start();
          try {
            dbWrapper.gpsValueRangeQuery(syntheticWorkload.getGpsValueRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a time range query on trip identification because ", e);
          }
          break;
        case HEATMAP_RANGE_QUERY:
            clientMonitoring.start();
          try {
            dbWrapper.heatmapRangeQuery(syntheticWorkload.getHeatmapRangeQuery());
          } catch (WorkloadException e) {
            LOGGER.error("Failed to execute a heat map range query because ", e);
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

