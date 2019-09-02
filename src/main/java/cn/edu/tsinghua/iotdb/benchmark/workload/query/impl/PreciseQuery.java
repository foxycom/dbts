package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class PreciseQuery {

  private List<DeviceSchema> deviceSchema;
  private long timestamp;
  private SensorGroup sensorGroup;

  public PreciseQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, long timestamp) {
    this.deviceSchema = deviceSchema;
    this.timestamp = timestamp;
    this.sensorGroup = sensorGroup;
  }

  public PreciseQuery(List<DeviceSchema> deviceSchema, long timestamp) {
    this.deviceSchema = deviceSchema;
    this.timestamp = timestamp;
  }

  public List<DeviceSchema> getDeviceSchema() {
    return deviceSchema;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public SensorGroup getSensorGroup() {
    return sensorGroup;
  }
}
