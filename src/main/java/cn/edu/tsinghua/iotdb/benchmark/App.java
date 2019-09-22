package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.SyntheticClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ClientMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ServerMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

  static {
    System.setProperty("logback.configurationFile", "conf/logback.xml");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
  private static ClientMonitoring clientMonitoring;

  public static void main(String[] args) throws SQLException {

    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      return;
    }
    Config config = ConfigParser.INSTANCE.config();
    switch (config.WORK_MODE) {
      case SYNTHETIC_BENCHMARK:
        testWithDefaultPath(config);
        break;
      case SERVER_MODE:
        serverMode(config);
        break;
      default:
        throw new SQLException("unsupported mode " + config.WORK_MODE);
    }
  }

  /** 按比例选择workload执行的测试 */
  private static void testWithDefaultPath(Config config) {

    MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
    mySql.initMysql(true);
    mySql.saveConfig();
    clientMonitoring = ClientMonitoring.INSTANCE;
    clientMonitoring.connect();

    Measurement measurement = new Measurement();
    measurement.setRemark("this is my remark");
    DBWrapper dbWrapper = new DBWrapper(measurement);

    // register schema if needed
    try {
      dbWrapper.init();
      if (config.ERASE_DATA) {
        try {
          dbWrapper.cleanup();
        } catch (TsdbException e) {
          LOGGER.error("Cleanup {} failed because ", config.DB_SWITCH, e);
        }
      }
      try {
        DataSchema dataSchema = DataSchema.getInstance();
        List<Bike> schemaList = new ArrayList<>();
        for (List<Bike> schemas : dataSchema.getClientBindSchema().values()) {
          schemaList.addAll(schemas);
        }
        dbWrapper.registerSchema(schemaList);
      } catch (TsdbException e) {
        LOGGER.error("Register {} schema failed because ", config.DB_SWITCH, e);
      }
    } catch (TsdbException e) {
      LOGGER.error("Initialize {} failed because ", config.DB_SWITCH, e);
    } finally {
      try {
        dbWrapper.close();
      } catch (TsdbException e) {
        LOGGER.error("Close {} failed because ", config.DB_SWITCH, e);
      }
    }
    // create CLIENT_NUMBER client threads to do the workloads
    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.CLIENTS_NUMBER);
    CyclicBarrier barrier =
        new CyclicBarrier(
            config.CLIENTS_NUMBER,
            () -> {
              Measurement loopMeasurement = new Measurement();
              for (Client client : clients) {
                loopMeasurement.mergeMeasurement(client.getMeasurement());
              }
              loopMeasurement.calculateMetrics();
              loopMeasurement.save();
            });
    long st;
    st = System.nanoTime();
    ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENTS_NUMBER);
    for (int i = 0; i < config.CLIENTS_NUMBER; i++) {
      SyntheticClient client = new SyntheticClient(i, downLatch, barrier);
      clients.add(client);
      executorService.submit(client);
    }
    finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
  }

  private static void finalMeasure(
      ExecutorService executorService,
      CountDownLatch downLatch,
      Measurement measurement,
      List<Measurement> threadsMeasurements,
      long st,
      List<Client> clients) {
    executorService.shutdown();

    try {
      // wait for all clients finish test
      downLatch.await();
      clientMonitoring.shutdown();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads finish.", e);
      Thread.currentThread().interrupt();
    }
    long en = System.nanoTime();
    LOGGER.info("All clients finished.");

    // sum up all the measurements and calculate statistics
    measurement.setElapseTime((en - st) / Constants.NANO_TO_SECONDS);
    for (Client client : clients) {
      threadsMeasurements.add(client.getMeasurement());
    }
    for (Measurement m : threadsMeasurements) {
      measurement.mergeMeasurement(m);
    }
    // must call calculateMetrics() before using the Metrics
    measurement.calculateMetrics();
    // output results
    measurement.showConfigs();
    measurement.showMeasurements();
    measurement.showMetrics();
    measurement.save();
  }

  private static void serverMode(Config config) {
    ServerMonitoring monitor = ServerMonitoring.INSTANCE;
    try {
      monitor.listen(config);
    } catch (IOException e) {
      LOGGER.error("Could not start server monitor because: {}", e.getMessage());
    }
  }
}
