package de.uni_passau.dbts.benchmark.measurement;

import de.uni_passau.dbts.benchmark.client.OperationController.Operation;
import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.mysql.MySqlLog;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmark results container.
 */
public class Measurement {

  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);
  private static final double[] MID_AVG_RANGE = {0.1, 0.9};
  private static Config config = ConfigParser.INSTANCE.config();

  /** List of latencies per operation. */
  private Map<Operation, List<Double>> operationLatencies;

  /** The sums of latencies per operation per thread. */
  private Map<Operation, List<Double>> getOperationLatencySumsList;

  /** The sums of latencies per operation. */
  private Map<Operation, Double> operationLatencySums;

  /** Time elapsed while registering a schema. */
  private double createSchemaTime;

  /** Loop index. */
  private int loopIndex = -1;

  /** Some remark to the results. */
  private String remark;

  /** MySQL connection. */
  private MySqlLog mysql;

  /** Total run time including schema registration and batch generation. */
  private double elapseTime;

  /** Numbers of successful operations. */
  private Map<Operation, Long> okOperationNumMap;

  /** Numbers of failed operations. */
  private Map<Operation, Long> failOperationNumMap;

  /** Number of processed data points per operation. */
  private Map<Operation, Long> okPointNumMap;

  /** Number of failed data points per operation. */
  private Map<Operation, Long> failPointNumMap;

  /**
   * Creates a measurement instance.
   */
  public Measurement() {
    mysql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
    mysql.initMysql(false);

    operationLatencies = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLatencies.put(operation, new ArrayList<>());
    }
    okOperationNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(operation, 0L);
    }
    failOperationNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      failOperationNumMap.put(operation, 0L);
    }
    okPointNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okPointNumMap.put(operation, 0L);
    }
    failPointNumMap = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      failPointNumMap.put(operation, 0L);
    }
    operationLatencySums = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLatencySums.put(operation, 0D);
    }
    getOperationLatencySumsList = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      getOperationLatencySumsList.put(operation, new ArrayList<>());
    }
  }

  /**
   * Returns the sums of latencies per operation.
   *
   * @return Sums of latencies per operation.
   */
  private Map<Operation, Double> getOperationLatencySums() {
    for (Operation operation : Operation.values()) {
      double sum = operationLatencies.get(operation).stream().mapToDouble(Double::doubleValue).sum();
      operationLatencySums.put(operation, sum);
    }
    return operationLatencySums;
  }

  /**
   * Returns the number of successful executions of an operation type.
   *
   * @param operation Operation type.
   * @return Number of successful executions.
   */
  public long getOkOperationNum(Operation operation) {
    return okOperationNumMap.get(operation);
  }

  /**
   * Returns the number of failed executions of an operation type.
   *
   * @param operation Operation type.
   * @return Number of failed executions.
   */
  public long getFailOperationNum(Operation operation) {
    return failOperationNumMap.get(operation);
  }

  /**
   * Returns the number of processed data points.
   *
   * @param operation Operation type.
   * @return Number of processed data points.
   */
  public long getOkPointNum(Operation operation) {
    return okPointNumMap.get(operation);
  }

  /**
   * Returns the number of failed data points.
   *
   * @param operation Operation type.
   * @return Number of failed points.
   */
  public long getFailPointNum(Operation operation) {
    return failPointNumMap.get(operation);
  }

  /**
   * Adds a number of processed data points by an operation.
   *
   * @param operation Operation type.
   * @param pointNum Number of processed data points.
   */
  public void addOkPointNum(Operation operation, int pointNum) {
    okPointNumMap.put(operation, okPointNumMap.get(operation) + pointNum);
  }

  /**
   * Adds a failed operation.
   *
   * @param operation Operation type.
   */
  public void addFailOperation(Operation operation) {
    failOperationNumMap.put(operation, failOperationNumMap.get(operation) + 1);
  }

  /**
   * Increments the loop counter.
   */
  public void incrementLoopIndex() {
    loopIndex++;
  }

  /**
   * Returns latencies lists for each operation type.
   *
   * @return Latencies for each operation.
   */
  private Map<Operation, List<Double>> getOperationLatencies() {
    return operationLatencies;
  }

  /**
   * Adds a successful operation.
   *
   * @param operation Operation type.
   * @param latency Latency.
   * @param okPointNum Number of processed points.
   */
  public void addOkOperation(Operation operation, double latency, long okPointNum) {
    okOperationNumMap.put(operation, okOperationNumMap.get(operation) + 1);
    operationLatencies.get(operation).add(latency);
    okPointNumMap.put(operation, okPointNumMap.get(operation) + okPointNum);

    double rate = okPointNum / (latency / 1000); // Points/s
    flushOkOperation(operation.getName(), latency, rate, okPointNum);
  }

  /**
   * Adds a failed operation.
   *
   * @param operation Operation type.
   * @param failPointNum Number of failed data points.
   */
  public void addFailOperation(Operation operation, long failPointNum) {
    failOperationNumMap.put(operation, failOperationNumMap.get(operation) + 1);
    failPointNumMap.put(operation, failPointNumMap.get(operation) + failPointNum);

    flushFailedOperation(operation.getName(), failPointNum);
  }

  /**
   * Saves a successful operation into the MySQL database.
   *
   * @param operation Operation type.
   * @param latency Latency.
   * @param rate Throughput of data points per second.
   * @param okPointNum Number of processed data points.
   */
  private void flushOkOperation(String operation, double latency, double rate, long okPointNum) {
    String clientName = Thread.currentThread().getName();
    mysql.saveClientMeasurement(
        clientName, operation, "OK", loopIndex, latency, elapseTime, rate, okPointNum, 0, remark);
  }

  /**
   * Saves a failed operation into the MySQL database.
   *
   * @param operation Operation type.
   * @param failPointNum Number of failed data points.
   */
  private void flushFailedOperation(String operation, long failPointNum) {
    String clientName = Thread.currentThread().getName();
    mysql.saveClientMeasurement(
        clientName, operation, "FAILED", loopIndex, elapseTime, 0, 0, 0, failPointNum, remark);
  }

  /**
   * Saves the overall measurement into the MySQL database. {@link #calculateMetrics()} must be
   * called beforehand.
   */
  public void save() {

    for (Operation operation : Operation.values()) {
      if (okOperationNumMap.get(operation) == 0 && failOperationNumMap.get(operation) == 0) {
        continue;
      }

      double accTime = Metric.MAX_THREAD_LATENCY_SUM.getOperationValueMap().get(operation);
      double accRate = 0;
      if (accTime != 0) {
        accRate = okPointNumMap.get(operation) * 1000 / accTime;
      }

      mysql.saveOverallMeasurement(
          operation.getName(),
          Metric.AVG_LATENCY.getOperationValueMap().get(operation),
          Metric.P99_LATENCY.getOperationValueMap().get(operation),
          Metric.MEDIAN_LATENCY.getOperationValueMap().get(operation),
          accTime,
          accRate,
          okPointNumMap.get(operation),
          failPointNumMap.get(operation),
          remark);
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Returns the remark.
   *
   * @return Remark.
   */
  public String getRemark() {
    return remark;
  }

  /**
   * Sets a new remark.
   *
   * @param remark New remark.
   */
  public void setRemark(String remark) {
    this.remark = remark;
  }

  /**
   * Returns the time elapsed while registering a schema.
   *
   * @return Time elapsed while registering a schema.
   */
  public double getCreateSchemaTime() {
    return createSchemaTime;
  }

  /**
   * Sets the time needed for a schema creation.
   *
   * @param createSchemaTime Latency.
   */
  public void setCreateSchemaTime(double createSchemaTime) {
    this.createSchemaTime = createSchemaTime;
  }

  /**
   * Returns the elapsed time since the beginning of a benchmark run.
   *
   * @return Elapsed time.
   */
  public double getElapseTime() {
    return elapseTime;
  }

  /**
   * Set the elapsed time since the beginning of a benchmark run.
   *
   * @param elapseTime Elapsed time.
   */
  public void setElapseTime(double elapseTime) {
    this.elapseTime = elapseTime;
  }

  /**
   * Merges results from another {@link Measurement} instance into this one.
   *
   * @param m {@link Measurement} instance to be merged.
   */
  public void mergeMeasurement(Measurement m) {
    for (Operation operation : Operation.values()) {
      operationLatencies.get(operation).addAll(m.getOperationLatencies().get(operation));
      getOperationLatencySumsList.get(operation).add(m.getOperationLatencySums().get(operation));
      okOperationNumMap.put(
          operation, okOperationNumMap.get(operation) + m.getOkOperationNum(operation));
      failOperationNumMap.put(
          operation, failOperationNumMap.get(operation) + m.getFailOperationNum(operation));
      okPointNumMap.put(operation, okPointNumMap.get(operation) + m.getOkPointNum(operation));
      failPointNumMap.put(operation, failPointNumMap.get(operation) + m.getFailPointNum(operation));
    }
  }

  /**
   * Calculates latencies and percentiles.
   */
  public void calculateMetrics() {
    for (Operation operation : Operation.values()) {
      List<Double> latencyList = operationLatencies.get(operation);
      if (!latencyList.isEmpty()) {
        int totalOps = latencyList.size();
        double sumLatency = 0;
        for (double latency : latencyList) {
          sumLatency += latency;
        }
        double avgLatency = sumLatency / totalOps;

        // Time elapsed since the start and the time the last thread completed the operation.
        double maxThreadLatencySum = getOperationLatencySumsList.get(operation).stream()
            .mapToDouble(Double::doubleValue).max().getAsDouble();

        Metric.MAX_THREAD_LATENCY_SUM.getOperationValueMap().put(operation, maxThreadLatencySum);
        Metric.AVG_LATENCY.getOperationValueMap().put(operation, avgLatency);
        latencyList.sort(new DoubleComparator());

        Metric.MIN_LATENCY.getOperationValueMap().put(operation, latencyList.get(0));
        Metric.MAX_LATENCY.getOperationValueMap().put(operation, latencyList.get(totalOps - 1));
        Metric.P10_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.10)));
        Metric.P25_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.25)));
        Metric.MEDIAN_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.50)));
        Metric.P75_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.75)));
        Metric.P90_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.90)));
        Metric.P95_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.95)));
        Metric.P99_LATENCY
            .getOperationValueMap()
            .put(operation, latencyList.get((int) (totalOps * 0.99)));
        double midAvgLatency = 0;
        double midSum = 0;
        int midCount = 0;
        for (int i = (int) (totalOps * MID_AVG_RANGE[0]);
            i < (int) (totalOps * MID_AVG_RANGE[1]);
            i++) {
          midSum += latencyList.get(i);
          midCount++;
        }
        if (midCount != 0) {
          midAvgLatency = midSum / midCount;
        } else {
          LOGGER.error(
              "Can not calculate mid-average latency because mid-operation number is zero.");
        }
        Metric.MID_AVG_LATENCY.getOperationValueMap().put(operation, midAvgLatency);
      }
    }
  }

  /**
   * Prints operation results into the console. {@link #calculateMetrics()} must be called beforehand.
   */
  public void showMeasurements() {
    System.out.println(Thread.currentThread().getName() + " measurements:");
    System.out.println("Test elapse time: " + String.format("%.2f", elapseTime) + " second");
    System.out.println("Create schema cost " + String.format("%.2f", createSchemaTime) + " second");

    System.out.println(
        "--------------------------------------------------Result Matrix--------------------------------------------------");
    String intervalString = "\t\t";
    System.out.println(
        "Operation\t\tokOperation\tokPoint\t\tfailOperation\tfailPoint\telapseRate\taccTime\taccRate");
    for (Operation operation : Operation.values()) {
      System.out.print(operation.getName() + intervalString);
      System.out.print(okOperationNumMap.get(operation) + intervalString);
      System.out.print(okPointNumMap.get(operation) + intervalString);
      System.out.print(failOperationNumMap.get(operation) + intervalString);
      System.out.print(failPointNumMap.get(operation) + intervalString);
      double accTime = Metric.MAX_THREAD_LATENCY_SUM.typeValueMap.get(operation) / 1000;
      String elapseRate = String.format("%.2f", okPointNumMap.get(operation) / elapseTime);
      double accRate = 0;
      if (accTime != 0) {
        accRate = okPointNumMap.get(operation) / accTime;
      }
      String rate = String.format("%.2f", accRate);
      System.out.print(elapseRate + intervalString);

      String time = String.format("%.2f", accTime);
      System.out.print(time + intervalString);
      System.out.println(rate + intervalString);
    }
    System.out.println(
        "-----------------------------------------------------------------------------------------------------------------");
  }

  /**
   * Prints operation latencies into the console. {@link #calculateMetrics()} must be called beforehand.
   */
  public void showMetrics() {
    System.out.println(
        "-----------------------------------------------Latency (ms) Matrix-----------------------------------------------");
    String intervalString = "\t";
    System.out.print("Operation" + intervalString);
    for (Metric metric : Metric.values()) {
      System.out.print(metric.name + intervalString);
    }
    System.out.println();
    for (Operation operation : Operation.values()) {
      System.out.print(operation.getName() + intervalString);
      for (Metric metric : Metric.values()) {
        System.out.print(
            String.format("%.2f", metric.typeValueMap.get(operation)) + intervalString);
      }
      System.out.println();
    }
    System.out.println(
        "-----------------------------------------------------------------------------------------------------------------");
  }

  /**
   * Metric types.
   */
  public enum Metric {
    AVG_LATENCY("AVG"),
    MID_AVG_LATENCY("MID_AVG"),
    MIN_LATENCY("MIN"),
    P10_LATENCY("P10"),
    P25_LATENCY("P25"),
    MEDIAN_LATENCY("MEDIAN"),
    P75_LATENCY("P75"),
    P90_LATENCY("P90"),
    P95_LATENCY("P95"),
    P99_LATENCY("P99"),
    MAX_LATENCY("MAX"),
    MAX_THREAD_LATENCY_SUM("MAX_SUM");

    Map<Operation, Double> typeValueMap;
    String name;

    /**
     * Initializes a metric.
     *
     * @param name The name of the metric.
     */
    Metric(String name) {
      this.name = name;
      typeValueMap = new EnumMap<>(Operation.class);
      for (Operation operation : Operation.values()) {
        typeValueMap.put(operation, 0D);
      }
    }

    /**
     * Returns the operation -&gt; value map.
     * @return Operation -&gt; value map.
     */
    public Map<Operation, Double> getOperationValueMap() {
      return typeValueMap;
    }

    /**
     * Returns the name of the metric.
     *
     * @return The name.
     */
    public String getName() {
      return name;
    }
  }

  static class DoubleComparator implements Comparator<Double> {

    @Override
    public int compare(Double a, Double b) {
      if (a < b) {
        return -1;
      } else if (Objects.equals(a, b)) {
        return 0;
      } else {
        return 1;
      }
    }
  }
}
