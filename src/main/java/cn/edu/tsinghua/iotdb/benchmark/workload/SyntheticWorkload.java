package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.utils.Sensors;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Point;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SyntheticWorkload implements IWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticWorkload.class);
    private static Config config = ConfigParser.INSTANCE.config();
  private static Random timestampRandom = new Random(config.DATA_SEED);
  private ProbTool probTool;
  private long maxTimestampIndex;
  private Random poissonRandom;
  private Random queryDeviceRandom;
  private Random querySensorGroupRandom;
  private Map<Operation, Long> operationLoops;
  private static Random random = new Random();

  public SyntheticWorkload(int clientId) {
    probTool = new ProbTool();
    maxTimestampIndex = 0;

    poissonRandom = new Random(config.DATA_SEED);
    queryDeviceRandom = new Random(config.QUERY_SEED + clientId);
    querySensorGroupRandom = new Random(config.QUERY_SEED);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  private static long getCurrentTimestamp(long stepOffset) {
    long timeStampOffset = config.POINT_STEP * stepOffset;
    if (config.USE_OVERFLOW) {
      timeStampOffset += random.nextDouble() * config.POINT_STEP;
    }
    long currentTimestamp = Constants.START_TIMESTAMP + timeStampOffset;
    if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
      currentTimestamp += (long) (config.POINT_STEP * timestampRandom.nextDouble());
    }
    return currentTimestamp;
  }

  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    batch.setTimeRange(config.loopTimeRange());
    for (Sensor sensor : deviceSchema.getSensors()) {
      addSensorData(sensor, batch, loopIndex);
    }

    //for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
    //  long stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;    // point step
      // addOneRowIntoBatch(deviceSchema, batch, stepOffset);
    //  addSensorData();
    //}
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private void setBatchTimeRange(Batch batch, long loopIndex) {
    long timeRange = config.loopTimeRange() * loopIndex;
    batch.setTimeRange(timeRange);
  }

  private Batch getDistOutOfOrderBatch(DeviceSchema deviceSchema) {
    Batch batch = new Batch();
    PossionDistribution possionDistribution = new PossionDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, poissonRandom)) {
        // generate overflow timestamp
        nextDelta = possionDistribution.getNextPossionDelta();
        stepOffset = maxTimestampIndex - nextDelta;
      } else {
        // generate normal increasing timestamp
        maxTimestampIndex++;
        stepOffset = maxTimestampIndex;
      }
      addOneRowIntoBatch(deviceSchema, batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  static void addOneRowIntoBatch(DeviceSchema deviceSchema, Batch batch, long stepOffset) {
    List<String> values = new ArrayList<>();
    long currentTimestamp;
    currentTimestamp = getCurrentTimestamp(stepOffset);
    for (Sensor sensor : deviceSchema.getSensors()) {

      // Different sensors may collect data with different frequencies
      if (sensor.hasValue(currentTimestamp)) {
        String value = sensor.getValue(currentTimestamp);
        values.add(value);
      }
    }
    batch.add(currentTimestamp, values);
  }

  static void addSensorData(Sensor sensor, Batch batch, long loopIndex) {
    long valuesNum = batch.getTimeRange() / sensor.getInterval();
    Integer valNum = Math.toIntExact(valuesNum);
    Point[] values = new Point[valNum];
    for (int i = 0; i < valuesNum; i++) {
      long stepOffset = loopIndex * valuesNum + i;    // point step
      long timestamp = sensor.getTimestamp(stepOffset);
      if (sensor.getFields().size() == 1) {
        String value = sensor.getValue(timestamp);
        values[i] = new Point(timestamp, value);
      } else {
        // TODO get value based on field
        String[] sensorValues = new String[sensor.getFields().size()];
        for (int v = 0; v < sensor.getFields().size(); v++) {
          sensorValues[v] = sensor.getValue(timestamp);
        }
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

  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    if (!config.USE_OVERFLOW) {
      long st = System.nanoTime();
      Batch batch = getOrderedBatch(deviceSchema, loopIndex);
      long en = System.nanoTime();
      double time = (en - st) / Constants.NANO_TO_SECONDS;
      LOGGER.info(String.format(Locale.US, "Generated one batch of size %d in %.3f s", batch.pointNum(), time));
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
          batch = getDistOutOfOrderBatch(deviceSchema);
          break;
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
      LOGGER.info("Generated one overflow batch of size {}", batch.pointNum());
      return batch;
    }
  }

  private List<DeviceSchema> getGpsQueryDeviceSchemaList() throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();

    int deviceId = queryDeviceRandom.nextInt(config.DEVICES_NUMBER);
    DeviceSchema deviceSchema = new DeviceSchema(deviceId);
    List<Sensor> gpsSensor = new ArrayList<>(1);

    // Add only GPS sensor
    for (Sensor sensor : config.SENSORS) {
      if (sensor.getSensorGroup().getName().contains("gps")) {
        gpsSensor.add(sensor);
        break;
      }
    }

    deviceSchema.setSensors(gpsSensor);
    queryDevices.add(deviceSchema);
    return queryDevices;
  }

  // TODO better handling for non-gps sensors
  private SensorGroup getSensorGroup() throws WorkloadException {
    checkQuerySchemaParams();
    Sensor sensor = Sensors.minInterval(config.SENSORS);
    return sensor.getSensorGroup();
  }

  private List<DeviceSchema> getQueryDeviceSchemaList() throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = 0; m < config.DEVICES_NUMBER; m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0; m < config.QUERY_DEVICE_NUM; m++) {
      DeviceSchema deviceSchema = new DeviceSchema(clientDevicesIndex.get(m));
      List<Sensor> sensors = deviceSchema.getSensors();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<Sensor> querySensors = new ArrayList<>();
      for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
        querySensors.add(sensors.get(i));
      }
      deviceSchema.setSensors(querySensors);
      queryDevices.add(deviceSchema);
    }
    return queryDevices;
  }

  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.QUERY_DEVICE_NUM > 0 && config.QUERY_DEVICE_NUM <= config.DEVICES_NUMBER)) {
      throw new WorkloadException("QUERY_DEVICE_NUM is not correct, please check.");
    }
    if (!(config.QUERY_SENSOR_NUM > 0 && config.QUERY_SENSOR_NUM <= config.SENSORS_NUMBER)) {
      throw new WorkloadException("QUERY_SENSOR_NUM is not correct, please check.");
    }
  }

  private long getQueryStartTimestamp() {
    long currentQueryLoop = operationLoops.get(Operation.PRECISE_QUERY);
    long timestampOffset = currentQueryLoop * config.STEP_SIZE * config.POINT_STEP;
    operationLoops.put(Operation.PRECISE_QUERY, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP + timestampOffset;
  }

  public PreciseQuery getPreciseQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long timestamp = getQueryStartTimestamp();
    return new PreciseQuery(queryDevices, sensorGroup, timestamp);
  }

  public RangeQuery getRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new RangeQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp);
  }

  @Override
  public RangeQuery getGpsRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getGpsQueryDeviceSchemaList();
    SensorGroup sensorGroup = Sensors.groupOfType("gps");
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new RangeQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp);
  }

  public ValueRangeQuery getGpsValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getGpsQueryDeviceSchemaList();
    SensorGroup sensorGroup = Sensors.groupOfType("gps");
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new ValueRangeQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp, config.QUERY_LOWER_LIMIT);
  }

  public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new ValueRangeQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp,
        config.QUERY_LOWER_LIMIT);
  }

  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new AggRangeQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN);
  }

  public AggValueQuery getAggValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    return new AggValueQuery(queryDevices, sensorGroup, config.QUERY_AGGREGATE_FUN, config.QUERY_LOWER_LIMIT);
  }

  public AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new AggRangeValueQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN, config.QUERY_LOWER_LIMIT);
  }

  public GroupByQuery getGroupByQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new GroupByQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp, config.QUERY_AGGREGATE_FUN,
        config.TIME_BUCKET);
  }

  public LatestPointQuery getLatestPointQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new LatestPointQuery(queryDevices, sensorGroup, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN);
  }

  public GpsRangeQuery getHeatmapRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getGpsQueryDeviceSchemaList();
    SensorGroup sensorGroup = getSensorGroup();
    SensorGroup gpsSensorGroup = Sensors.groupOfType("gps");
    long startTimestamp = getQueryStartTimestamp();
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new GpsRangeQuery(queryDevices, sensorGroup, gpsSensorGroup, startTimestamp, endTimestamp);
  }


}
