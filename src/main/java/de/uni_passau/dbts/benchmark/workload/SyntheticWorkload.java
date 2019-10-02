package de.uni_passau.dbts.benchmark.workload;

import de.uni_passau.dbts.benchmark.client.OperationController.Operation;
import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.distribution.PossionDistribution;
import de.uni_passau.dbts.benchmark.function.GeoFunction;
import de.uni_passau.dbts.benchmark.utils.NameGenerator;
import de.uni_passau.dbts.benchmark.utils.Sensors;
import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.ingestion.DataPoint;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;
import de.uni_passau.dbts.benchmark.workload.schema.GpsSensor;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Generates synthetic workload on a database.
 */
public class SyntheticWorkload implements Workload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticWorkload.class);

  /** Config singleton. */
  private static Config config = ConfigParser.INSTANCE.config();

  /** Random timestamp generator. */
  private static Random timestampRandom = new Random(config.DATA_SEED);

  /** Random name generator. */
  private NameGenerator nameGenerator = NameGenerator.INSTANCE;

  /** Max timestamp index. */
  private long maxTimestampIndex;

  /** Random poisson distribution. */
  private Random poissonRandom;

  /** Random bike generator. */
  private Random queryDeviceRandom;

  private Map<Operation, Long> operationLoops;

  /**
   * Creates an instance of synthetic workload generator.
   *
   * @param clientId ID of the thread which uses this generator.
   */
  public SyntheticWorkload(int clientId) {
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
    if (config.RANDOM_TIMESTAMP_INTERVAL) {
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
      if (poissonRandom.nextDouble() < config.OVERFLOW_RATIO) {
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
   * Adds readings of a sensor for the entire time range of a batch.
   *
   * @param sensor Sensor.
   * @param batch Batch of data points.
   * @param loopIndex Loop index.
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
    DataPoint[] values = new DataPoint[valNum];
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
        values[i] = new DataPoint(timestamp, value);
      } else {
        String[] sensorValues = sensor.getValues(timestamp, config.DB_SWITCH);
        values[i] = new DataPoint(timestamp, sensorValues);
      }
    }

    batch.add(sensor, values);
  }

  /**
   * To be implemented in future updates.
   *
   * @return null
   */
  private Batch getLocalOutOfOrderBatch() {
    return null;
  }

  /**
   * To be implemented in future updates.
   *
   * @return null
   */
  private Batch getGlobalOutOfOrderBatch() {
    return null;
  }

  /**
   * Generates a batch of data points on the fly.
   *
   * @param bike The bike the batch of points should belong to.
   * @param loopIndex Loop index.
   * @return A batch of data points.
   * @throws WorkloadException if an error occurs while generating a batch.
   */
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
          /*batch = getLocalOutOfOrderBatch();*/
          throw new NotImplementedException(
              "localOutOfOrderBatch is not implemented, yet.");
        case 1:
          /*batch = getGlobalOutOfOrderBatch();*/
          throw new NotImplementedException(
              "globalOutOfOrder is not implemented, yet.");
        case 2:
          /*batch = getDistOutOfOrderBatch(bike);*/
          throw new NotImplementedException(
              "distOutOfOrderBatch is not implemented, yet.");
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
      /*LOGGER.info("Generated one overflow batch of size {}", batch.pointNum());
      return batch;*/
    }
  }

  /**
   * Randomly generates a list of bikes to query for.
   *
   * @return List of bikes.
   * @throws WorkloadException if an error occurs.
   */
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
