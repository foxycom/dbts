package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class RangeQuery {

  private List<DeviceSchema> deviceSchema;
  private long startTimestamp;
  private long endTimestamp;
  private SensorGroup sensorGroup;

  public RangeQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, long startTimestamp, long endTimestamp) {
    this.deviceSchema = deviceSchema;
    this.sensorGroup = sensorGroup;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  public RangeQuery(List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp) {
    this.deviceSchema = deviceSchema;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  public List<DeviceSchema> getDeviceSchema() {
    return deviceSchema;
  }

  public SensorGroup getSensorGroup() {
    return sensorGroup;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }


}
