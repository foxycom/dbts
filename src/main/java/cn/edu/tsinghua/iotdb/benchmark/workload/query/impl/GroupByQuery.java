package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.enums.Aggregation;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class GroupByQuery extends RangeQuery {
  // use startTimestamp to be the segment start time
  private Aggregation aggrFunc;
  private long granularity;

  public Aggregation getAggrFunc() {
    return aggrFunc;
  }

  public long getGranularity() {
    return granularity;
  }

  public GroupByQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, long startTimestamp, long endTimestamp,
                      Aggregation aggrFunc, long granularity) {
    super(deviceSchema, sensorGroup, startTimestamp, endTimestamp);
    this.aggrFunc = aggrFunc;
    this.granularity = granularity;
  }

  public GroupByQuery(
          List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
          Aggregation aggrFunc, long granularity) {
    super(deviceSchema, startTimestamp, endTimestamp);
    this.aggrFunc = aggrFunc;
    this.granularity = granularity;
  }
}
