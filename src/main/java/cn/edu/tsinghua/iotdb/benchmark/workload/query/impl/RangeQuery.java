package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class RangeQuery extends Query {

  private long startTimestamp;
  private long endTimestamp;

  public RangeQuery(List<DeviceSchema> deviceSchemas, SensorGroup sensorGroup, long startTimestamp, long endTimestamp) {
    super(deviceSchemas, sensorGroup);
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  public RangeQuery(List<DeviceSchema> deviceSchemas, long startTimestamp, long endTimestamp) {
    super(deviceSchemas);
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
  }

  public long getStartTimestamp() {
    return startTimestamp;
  }

  public long getEndTimestamp() {
    return endTimestamp;
  }


}
