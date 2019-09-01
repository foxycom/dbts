package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class ValueRangeQuery extends RangeQuery {

  private double valueThreshold;

  public ValueRangeQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, long startTimestamp,
                         long endTimestamp, double valueThreshold) {
    super(deviceSchema, sensorGroup, startTimestamp, endTimestamp);
    this.valueThreshold = valueThreshold;
  }

  public ValueRangeQuery(List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
                         double valueThreshold) {
    super(deviceSchema, startTimestamp, endTimestamp);
    this.valueThreshold = valueThreshold;
  }

  public double getValueThreshold() {
    return valueThreshold;
  }

}
