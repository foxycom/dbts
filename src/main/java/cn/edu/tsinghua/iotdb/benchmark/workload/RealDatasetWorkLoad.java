package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.GeolifeReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.ReddReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.TDriveReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;

import java.util.ArrayList;
import java.util.List;

public class RealDatasetWorkLoad implements IWorkload {

  private BasicReader reader;

  private Config config;
  private List<Bike> bikeList;
  private long startTime;
  private long endTime;

  /**
   * write test.
   *
   * @param files real dataset files
   * @param config config
   */
  public RealDatasetWorkLoad(List<String> files, Config config) {
    switch (config.DATA_SET) {
      case TDRIVE:
        reader = new TDriveReader(config, files);
        break;
      case REDD:
        reader = new ReddReader(config, files);
        break;
      case GEOLIFE:
        reader = new GeolifeReader(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }
  }

  /**
   * read test.
   *
   * @param config config
   */
  public RealDatasetWorkLoad(Config config) {
    this.config = config;

    //init sensor list
    List<Sensor> sensorList = new ArrayList<>();
    for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
      sensorList.add(config.FIELDS.get(i));
    }

    //init device schema list
    bikeList = new ArrayList<>();
    for (int i = 1; i <= config.QUERY_DEVICE_NUM; i++) {
      String deviceIdStr = "" + i;
      Bike bike = new Bike(calGroupIdStr(deviceIdStr, config.DEVICE_GROUPS_NUMBER),
          deviceIdStr, sensorList);
      bikeList.add(bike);
    }

    //init startTime, endTime
    startTime = config.REAL_QUERY_START_TIME;
    endTime = config.REAL_QUERY_STOP_TIME;

  }

  public Batch getOneBatch() {
    if (reader.hasNextBatch()) {
      return reader.nextBatch();
    } else {
      return null;
    }
  }

  @Override
  public Batch getOneBatch(Bike bike, long loopIndex) throws WorkloadException {
    throw new WorkloadException("not support in real data workload.");
  }

  @Override
  public Query getQuery(String sensorType) throws WorkloadException {
    return null;
  }


  static String calGroupIdStr(String deviceId, int groupNum) {
    return String.valueOf(deviceId.hashCode() % groupNum);
  }

}
