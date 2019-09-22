package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.GeoFunction;
import cn.edu.tsinghua.iotdb.benchmark.utils.NameGenerator;
import cn.edu.tsinghua.iotdb.benchmark.utils.Sensors;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Point;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GeoPoint;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GpsSensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SyntheticWorkload implements Workload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticWorkload.class);
  private static Config config = ConfigParser.INSTANCE.config();
  private static Random timestampRandom = new Random(config.DATA_SEED);
  private ProbTool probTool;
  private NameGenerator nameGenerator = NameGenerator.INSTANCE;
  private long maxTimestampIndex;
  private Random poissonRandom;
  private Random queryDeviceRandom;
  private Map<Operation, Long> operationLoops;

  /**
   * Creates an instance of synthetic workload generator.
   *
   * @param clientId ID of the thread which uses this generator.
   */
  public SyntheticWorkload(int clientId) {
    probTool = new ProbTool();
    maxTimestampIndex = 0;

    poissonRandom = new Random(config.DATA_SEED);
    queryDeviceRandom = new Random(config.QUERY_SEED + clientId);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  /**
   * Returns the timestamp for the given step relative to the configured start timestamp.
   *
   * @param stepOffset Step number.
   * @return Timestamp for the given <code>stepOffset</code>.
   */
  private static long getCurrentTimestamp(long stepOffset) {
    long currentTimestamp = Constants.START_TIMESTAMP + stepOffset;
    if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
      currentTimestamp += (long) timestampRandom.nextDouble();
    }
    return currentTimestamp;
  }

  /**
   * Generates a batch with entries being chronologically ordered.
   *
   * @param bike ID of the bike which the batch is generated for.
   * @param loopIndex Index of the current loop.
   * @return An chronologically ordered batch.
   */
  private Batch getOrderedBatch(Bike bike, long loopIndex) {
    Batch batch = new Batch(bike);
    batch.setTimeRange(config.loopTimeRange());
    for (Sensor sensor : bike.getSensors()) {
      addSensorData(sensor, batch, loopIndex);
    }
    return batch;
  }

  /**
   * Generates of batch with entries being chronologically mixed up.
   *
   * <p>TODO: implement different sampling frequencies.
   *
   * @param bike ID of the bike which the batch is generated for.
   * @return A chronologically mixed up batch.
   */
  private Batch getDistOutOfOrderBatch(Bike bike) {
    Batch batch = new Batch(bike);
    PossionDistribution possionDistribution = new PossionDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, poissonRandom)) {
        // Generates overflow timestamp.
        nextDelta = possionDistribution.getNextPossionDelta();
        stepOffset = maxTimestampIndex - nextDelta;
      } else {
        // Generates normal increasing timestamp.
        maxTimestampIndex++;
        stepOffset = maxTimestampIndex;
      }
      // addOneRowIntoBatch(bike, batch, stepOffset);
    }
    batch.setBike(bike);
    return batch;
  }

  /**
   * TODO assumption that GPS sensor is always slower than the fastest sensor
   *
   * @param sensor
   * @param batch
   * @param loopIndex
   */
  static void addSensorData(Sensor sensor, Batch batch, long loopIndex) {
    long valuesNum;
    Sensor minIntervalSensor = Sensors.minInterval(config.SENSORS);
    if (sensor instanceof GpsSensor) {
      valuesNum = batch.getTimeRange() / minIntervalSensor.getInterval();
    } else {
      valuesNum = batch.getTimeRange() / sensor.getInterval();
    }
    Integer valNum = Math.toIntExact(valuesNum);
    Point[] values = new Point[valNum];
    for (int i = 0; i < valuesNum; i++) {
      long stepOffset = loopIndex * valuesNum + i; // point step
      long timestamp;
      if (sensor instanceof GpsSensor) {
        timestamp = minIntervalSensor.getTimestamp(stepOffset);
      } else {
        timestamp = sensor.getTimestamp(stepOffset);
      }
      if (sensor.getFields().size() == 1) {
        String value = sensor.getValue(timestamp, config.DB_SWITCH);
        values[i] = new Point(timestamp, value);
      } else {
        String[] sensorValues = sensor.getValues(timestamp, config.DB_SWITCH);
        values[i] = new Point(timestamp, sensorValues);
      }
    }

    batch.add(sensor, values);
  }

  private Batch getLocalOutOfOrderBatch() {
    return null;
  }

  private Batch getGlobalOutOfOrderBatch() {
    return null;
  }

  public Batch getOneBatch(Bike bike, long loopIndex) throws WorkloadException {
    if (!config.USE_OVERFLOW) {
      long st = System.nanoTime();
      Batch batch = getOrderedBatch(bike, loopIndex);
      long en = System.nanoTime();
      double time = (en - st) / Constants.NANO_TO_SECONDS;
      LOGGER.info(
          String.format(
              Locale.US, "Generated one batch of size %d in %.3f s", batch.pointNum(), time));
      return batch;
    } else {
      Batch batch;
      switch (config.OVERFLOW_MODE) {
        case 0:
          batch = getLocalOutOfOrderBatch();
          break;
        case 1:
          batch = getGlobalOutOfOrderBatch();
          break;
        case 2:
          /*batch = getDistOutOfOrderBatch(bike);*/
          throw new NotImplementedException(
              "distOutOfOrderBatch is not implemented in the moment.");
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
      LOGGER.info("Generated one overflow batch of size {}", batch.pointNum());
      return batch;
    }
  }

  private List<Bike> getQueryBikesList() throws WorkloadException {
    checkQuerySchemaParams();
    List<Bike> queryDevices = new ArrayList<>();
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = 0; m < config.DEVICES_NUMBER; m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0; m < config.QUERY_DEVICE_NUM; m++) {
      Bike bike = new Bike(clientDevicesIndex.get(m), nameGenerator.getName());
      List<Sensor> sensors = bike.getSensors();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<Sensor> querySensors = new ArrayList<>();
      for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
        querySensors.add(sensors.get(i));
      }
      bike.setSensors(querySensors);
      queryDevices.add(bike);
    }
    return queryDevices;
  }

  /**
   * Checks if the configured number of devices and sensors per query is valid.
   *
   * @throws WorkloadException if number of query devices and/or sensors is configured invalidly.
   */
  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.QUERY_DEVICE_NUM > 0 && config.QUERY_DEVICE_NUM <= config.DEVICES_NUMBER)) {
      throw new WorkloadException(
          "The number of devices per query should be smaller than the overall devices number.");
    }
    if (!(config.QUERY_SENSOR_NUM > 0 && config.QUERY_SENSOR_NUM <= config.SENSORS_NUMBER)) {
      throw new WorkloadException(
          "The number of sensors per query should be smaller than the overall sensors number.");
    }
  }

  /**
   * Generates the left temporal boundary of a time range for a query.
   *
   * @return The left temporal boundary.
   */
  private long getQueryStartTimestamp() {
    long currentQueryLoop = operationLoops.get(Operation.PRECISE_POINT);
    long timestampOffset = currentQueryLoop * config.STEP_SIZE;
    operationLoops.put(Operation.PRECISE_POINT, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP + timestampOffset;
  }

  /**
   * Generates a random geospatial location.
   *
   * @return Random location.
   */
  private GeoPoint getRandomLocation() {
    GeoFunction function = new GeoFunction(1234);
    return function.get(Constants.SPAWN_POINT);
  }

  /**
   * Generates query parameters.
   *
   * @param sensorType The type of sensor to the use in queries, e.g., humidity.
   * @return Query parameters object.
   * @throws WorkloadException if the configuration is defined invalidly.
   */
  public Query getQuery(String sensorType) throws WorkloadException {
    List<Bike> bikes = getQueryBikesList();
    Sensor sensor = Sensors.ofType(sensorType);
    Sensor gpsSensor = Sensors.ofType("gps");
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    GeoPoint randomLocation = getRandomLocation();
    Query query = new Query();
    query =
        query
            .setSensor(sensor)
            .setGpsSensor(gpsSensor)
            .setBikes(bikes)
            .setStartTimestamp(startTimestamp)
            .setEndTimestamp(endTimestamp)
            .setAggrFunc(config.QUERY_AGGREGATE_FUN)
            .setThreshold(config.QUERY_LOWER_LIMIT)
            .setLocation(randomLocation);
    return query;
  }
}
