package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.enums.Aggregation;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class AggRangeValueQuery extends AggRangeQuery {

  private double valueThreshold;

  public AggRangeValueQuery(List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
          Aggregation aggrFunc, double valueThreshold) {
    super(deviceSchema, startTimestamp, endTimestamp, aggrFunc);
    this.valueThreshold = valueThreshold;
  }

  public AggRangeValueQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, long startTimestamp,
                            long endTimestamp, Aggregation aggrFunc, double valueThreshold) {
    super(deviceSchema, sensorGroup, startTimestamp, endTimestamp, aggrFunc);
    this.valueThreshold = valueThreshold;
  }

  public double getValueThreshold() {
    return valueThreshold;
  }

}
