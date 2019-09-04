package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class GpsValueRangeQuery extends GpsRangeQuery {
    private double valueThreshold;

    public GpsValueRangeQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, SensorGroup gpsSensorGroup,
                              long startTimestamp, long endTimestamp, double valueThreshold) {
        super(deviceSchema, sensorGroup, gpsSensorGroup, startTimestamp, endTimestamp);
        this.valueThreshold = valueThreshold;
    }

    public double getValueThreshold() {
        return valueThreshold;
    }
}
