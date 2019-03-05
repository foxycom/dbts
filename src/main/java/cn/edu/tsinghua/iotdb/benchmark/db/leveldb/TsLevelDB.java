package cn.edu.tsinghua.iotdb.benchmark.db.leveldb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import edu.tsinghua.k1.BaseTimeSeriesDBFactory;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.iq80.leveldb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsLevelDB implements IDatebase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsLevelDB.class);
  ITimeSeriesDB timeSeriesDB = null;
  private static Config config;
  private List<Point> points;
  private Map<String, String> mp;
  private long labID;
  private MySqlLog mySql;
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private Random sensorRandom;
  private Random timestampRandom;
  private ProbTool probTool;
  private final double unitTransfer = 1000000000.0;
  private Options options;

  public TsLevelDB(long labID) {
    config = ConfigDescriptor.getInstance().getConfig();
    points = new ArrayList<>();
    mp = new HashMap<>();
    mySql = new MySqlLog();
    this.labID = labID;
    sensorRandom = new Random(1 + config.QUERY_SEED);
    timestampRandom = new Random(2 + config.QUERY_SEED);
    probTool = new ProbTool();
    mySql.initMysql(labID);

    timeSeriesDB = LevelDB.getInstance().getTimeSeriesDB();

  }


  @Override
  public void init() {
//    //delete old data
//    File file = new File(config.GEN_DATA_FILE_PATH);
//    try {
//      BaseTimeSeriesDBFactory.getInstance().destroy(file, options);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }

  @Override
  public void createSchema() throws SQLException {

  }

  @Override
  public long getLabID() {
    return 0;
  }

  @Override
  public void insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
    ITimeSeriesWriteBatch batch = timeSeriesDB.createBatch();
    System.out.println("inserting one batch begin...");
    long errorNum = 0;
    try {

      if (!config.IS_OVERFLOW) {
        for (int i = 0; i < config.CACHE_NUM; i++) {
          addBatch(device, loopIndex, batch, i);
        }
      } else {
        int shuffleSize = (int) (config.OVERFLOW_RATIO * config.CACHE_NUM);
        int[] shuffleSequence = new int[shuffleSize];
        for (int i = 0; i < shuffleSize; i++) {
          shuffleSequence[i] = i;
        }

        int tmp = shuffleSequence[shuffleSize - 1];
        shuffleSequence[shuffleSize - 1] = shuffleSequence[0];
        shuffleSequence[0] = tmp;

        for (int i = 0; i < shuffleSize; i++) {
          addBatch(device, loopIndex, batch, i);
        }
        for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
          addBatch(device, loopIndex, batch, i);
        }
      }
      long startTime = System.nanoTime();
      try {
        timeSeriesDB.write(batch);
      } catch (Exception e) {
        LOGGER.error("Batch one insert failed because: ", e);
        errorNum += config.CACHE_NUM * config.SENSOR_NUMBER;
      }
      long endTime = System.nanoTime();
      long costTime = endTime - startTime;
      latencies.add(costTime);
      if (errorNum > 0) {
        LOGGER.error("Batch insert failed, the failed number is {}! ", errorNum);
      } else {
        LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
            Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
            (totalTime.get() + costTime) / unitTransfer,
            (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
        totalTime.set(totalTime.get() + costTime);
      }
      errorCount.set(errorCount.get() + errorNum);
      mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer,
          totalTime.get() / unitTransfer, errorNum,
          config.REMARK);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addBatch(String device, int loopIndex, ITimeSeriesWriteBatch batch, int i) {
    for (String sensor : config.SENSOR_CODES) {
      long timestamp = getTimestamp(loopIndex, i);
      byte[] dataBytes = getDataBytes(loopIndex, i, sensor);
      String timeSeries = getSeries(device, sensor);
      batch.write(timeSeries, timestamp, dataBytes);
    }
  }

  private long getTimestamp(int loopIndex, int i) {
    long currentTime =
        Constants.START_TIMESTAMP + config.POINT_STEP * (loopIndex * config.CACHE_NUM + i);
    if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
      currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
    }
    return currentTime;
  }

  private String getSeries(String device, String sensor) {
    StringBuilder builder = new StringBuilder();
    String groupDevicePath = getGroupDevicePath(device);
    builder.append(Constants.ROOT_SERIES_NAME).append(".").append(groupDevicePath).append(".");
    builder.append(sensor);
    return builder.toString();
  }

  public static byte[] double2Bytes(double d) {
    long value = Double.doubleToRawLongBits(d);
    byte[] byteRet = new byte[8];
    for (int i = 0; i < 8; i++) {
      byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
    }
    return byteRet;
  }

  private byte[] getDataBytes(int loopIndex, int i, String sensor) {
    long currentTime =
        Constants.START_TIMESTAMP + config.POINT_STEP * (loopIndex * config.CACHE_NUM + i);
    if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
      currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
    }
    FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
    return double2Bytes(Function.getValueByFuntionidAndParam(param, currentTime).doubleValue());
  }

  private String getGroupDevicePath(String device) {
    String[] spl = device.split("_");
    int deviceIndex = Integer.parseInt(spl[1]);
    int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
    int groupIndex = deviceIndex / groupSize;
    return "group_" + groupIndex + "." + device;
  }

  @Override
  public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

  }

  @Override
  public void close() throws SQLException {
    LOGGER.info("The leveldb will be closed.................begin");
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    try {
      timeSeriesDB.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    LOGGER.info("The leveldb will be closed.................end");
  }

  @Override
  public long getTotalTimeInterval() throws SQLException {
    return 0;
  }

  public static double bytes2Double(byte[] arr) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (arr[i] & 0xff)) << (8 * i);
    }
    return Double.longBitsToDouble(value);
  }

  public static long bytes2Long(byte[] arr) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value |= ((long) (arr[i] & 0xff)) << (8 * i);
    }
    return value;
  }

  @Override
  public void executeOneQuery(List<Integer> devices, int index, long startTime,
      QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
    List<String> list = new ArrayList<String>();
    for (String sensor : config.SENSOR_CODES) {
      list.add(sensor);
    }
    Collections.shuffle(list, sensorRandom);
    String timeSeries = Constants.ROOT_SERIES_NAME + "." + getGroupDevicePath("d_" + devices.get(0)) + "." + list.get(0);
    // query data with range which contains data
    TimeSeriesDBIterator dbIterator = timeSeriesDB.iterator(timeSeries, startTime,startTime + config.QUERY_INTERVAL);
    LOGGER.info("query {}, start time {}, end time {}", timeSeries, startTime, startTime + config.QUERY_INTERVAL);
    int line = 0;
    long startTimeStamp = System.nanoTime();
    while(dbIterator.hasNext()){
      line++;
      Map.Entry<byte[], byte[]> entry = dbIterator.next();
      double value = bytes2Double(entry.getValue());
      byte[] timeBytes = new byte[8];
      for(int i = 0; i < 8; i++){
        timeBytes[i] = entry.getKey()[11 - i];
      }
      long time = bytes2Long(timeBytes);
      // LOGGER.info("query result {}: [{}, {}]", line, time, value);
    }
    long endTimeStamp = System.nanoTime();
    long latency = endTimeStamp - startTimeStamp;
    latencies.add(latency);
    client.setTotalPoint(client.getTotalPoint() + line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM);
    client.setTotalTime(client.getTotalTime() + latency);
    LOGGER.info(
        "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
            + "TotalTime {}s with totalPoint {} rate is {}points/s",
        Thread.currentThread().getName(), index, (latency / 1000.0) / 1000000.0,
        line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM,
        line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM * 1000.0 / (latency / 1000000.0),
        (client.getTotalTime() / 1000.0) / 1000000.0, client.getTotalPoint(),
        client.getTotalPoint() * 1000.0f / (client.getTotalTime() / 1000000.0));
    mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM,
        (latency / 1000.0f) / 1000000.0, config.REMARK);
  }

  @Override
  public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex,
      ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies)
      throws SQLException {

  }

  @Override
  public long count(String group, String device, String sensor) {
    return 0;
  }

  @Override
  public void createSchemaOfDataGen() throws SQLException {

  }

  @Override
  public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

  }

  @Override
  public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

  }

  @Override
  public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex,
      Random random, ArrayList<Long> latencies) throws SQLException {
    return 0;
  }

  @Override
  public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime,
      ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random,
      ArrayList<Long> latencies) throws SQLException {
    return 0;
  }
}
