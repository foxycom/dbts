package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class PreciseQuery extends Query {
  private long timestamp;

  public PreciseQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, long timestamp) {
    super(deviceSchema, sensorGroup);
    this.timestamp = timestamp;
  }

  public PreciseQuery(List<DeviceSchema> deviceSchema, long timestamp) {
    super(deviceSchema);
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
