package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.enums.Aggregation;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class GpsAggValueRangeQuery extends GpsValueRangeQuery {
    private Aggregation aggrFunc;
    public GpsAggValueRangeQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, SensorGroup gpsSensorGroup,
                                 long startTimestamp, long endTimestamp, double valueThreshold, Aggregation aggrFunc) {
        super(deviceSchema, sensorGroup, gpsSensorGroup, startTimestamp, endTimestamp, valueThreshold);
        this.aggrFunc = aggrFunc;
    }

    public Aggregation getAggrFunc() {
        return aggrFunc;
    }
}
