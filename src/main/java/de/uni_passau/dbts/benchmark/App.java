package de.uni_passau.dbts.benchmark;

import de.uni_passau.dbts.benchmark.client.Client;
import de.uni_passau.dbts.benchmark.client.SyntheticClient;
import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.measurement.Measurement;
import de.uni_passau.dbts.benchmark.mysql.MySqlLog;
import de.uni_passau.dbts.benchmark.monitor.MonitoringClient;
import de.uni_passau.dbts.benchmark.monitor.MonitoringServer;
import de.uni_passau.dbts.benchmark.tsdb.DBWrapper;
import de.uni_passau.dbts.benchmark.tsdb.TsdbException;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.DataSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the starting point of the benchmark tool. It decides which working mode to take
 * based on the configuration file, which should be passed as a command line parameter, see
 * {@link #main(String[])}. Currently only two modes are supported: benchmark with synthetic data
 * and server mode. The former works with synthetic data generated on the fly, and executes
 * predefined scenarios. The latter should be used on the machine running the benchmarked DBMS
 * instance, as it collects machine's KPIs and sends them over TCP to the main benchmark tool, which
 * runs in synthetic mode. See {@link SyntheticClient} and {@link MonitoringServer} for more.
 */
public class App {

  static {
    System.setProperty("logback.configurationFile", "conf/logback.xml");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
  private static MonitoringClient monitoringClient;

  /**
   * Entry point of the benchmark tool. The input params should at least contain the -cf argument
   * pointing to the location of a configuration file, e.g., -cf /home/user/dbts/conf/memsql.xml.
   * See {@link CommandCli#init(String[])}.
   *
   * @param args CLI params.
   */
  public static void main(String[] args) {

    CommandCli cli = new CommandCli();
    if (!cli.init(args)) {
      return;
    }
    Config config = ConfigParser.INSTANCE.config();
    switch (config.WORK_MODE) {
      case SYNTHETIC_BENCHMARK:
        syntheticBenchmark(config);
        break;
      case SERVER_MODE:
        serverMode(config);
        break;
      default:
        throw new IllegalArgumentException("Unsupported mode " + config.WORK_MODE);
    }
  }

  /**
   * Executes benchmarks with synthetic data.
   *
   * @param config Configuration params instance.
   */
  private static void syntheticBenchmark(Config config) {

    MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
    mySql.initMysql(true);
    mySql.saveConfig();
    monitoringClient = MonitoringClient.INSTANCE;
    monitoringClient.connect();

    Measurement measurement = new Measurement();
    DBWrapper dbWrapper = new DBWrapper(measurement);

    try {
      dbWrapper.init();
      if (config.ERASE_DATA) {
        try {
          dbWrapper.cleanup();
        } catch (TsdbException e) {
          LOGGER.error("Could not erase {} data because ", config.DB_SWITCH, e);
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
        LOGGER.error("Registering {} schema failed because ", config.DB_SWITCH, e);
      }
    } catch (TsdbException e) {
      LOGGER.error("Initialization of {} failed because ", config.DB_SWITCH, e);
    } finally {
      try {
        dbWrapper.close();
      } catch (TsdbException e) {
        LOGGER.error("Could not close {} connection because ", config.DB_SWITCH, e);
      }
    }

    List<Measurement> threadsMeasurements = new ArrayList<>();
    List<Client> clients = new ArrayList<>();
    CountDownLatch downLatch = new CountDownLatch(config.CLIENTS_NUMBER);
    CyclicBarrier barrier =
        new CyclicBarrier(
            config.CLIENTS_NUMBER,
            () -> {

              // Calculates and saves thread metrics after each loop.
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

  /**
   * Calculates overall measurements after each thread has finished its tests.
   *
   * @param executorService Threads executor service.
   * @param downLatch Threads down latch.
   * @param measurement The overall measurement.
   * @param threadsMeasurements Thread-specific measurements.
   * @param st Start timestamp.
   * @param clients Threads list.
   */
  private static void finalMeasure(
      ExecutorService executorService,
      CountDownLatch downLatch,
      Measurement measurement,
      List<Measurement> threadsMeasurements,
      long st,
      List<Client> clients) {
    executorService.shutdown();

    try {
      // Waits for all clients to finish tests.
      downLatch.await();

      monitoringClient.shutdown();
    } catch (InterruptedException e) {
      LOGGER.error("Exception occurred during waiting for all threads to finish.", e);
      Thread.currentThread().interrupt();
    }
    long en = System.nanoTime();
    LOGGER.info("All clients finished.");

    // Sums up all the measurements and calculates statistics.
    measurement.setElapseTime((en - st) / Constants.NANO_TO_SECONDS);

    for (Client client : clients) {
      threadsMeasurements.add(client.getMeasurement());
    }
    for (Measurement m : threadsMeasurements) {
      measurement.mergeMeasurement(m);
    }

    measurement.calculateMetrics();
    measurement.showMeasurements();
    measurement.showMetrics();
    measurement.save();
  }

  /**
   * Starts dbts in server mode.
   *
   * @param config Configuration params instance.
   */
  private static void serverMode(Config config) {
    MonitoringServer monitor = MonitoringServer.INSTANCE;
    try {
      monitor.listen(config);
    } catch (IOException e) {
      LOGGER.error("Could not start server monitor because: {}", e.getMessage());
    }
  }
}
