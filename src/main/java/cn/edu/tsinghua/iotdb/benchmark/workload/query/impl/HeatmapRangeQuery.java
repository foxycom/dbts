package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class HeatmapRangeQuery extends RangeQuery {
    private SensorGroup gpsSensorGroup;

    public HeatmapRangeQuery(List<DeviceSchema> deviceSchema, SensorGroup sensorGroup, SensorGroup gpsSensorGroup,
                             long startTimestamp, long endTimestamp) {
        super(deviceSchema, sensorGroup, startTimestamp, endTimestamp);
        this.gpsSensorGroup = gpsSensorGroup;
    }

    public SensorGroup getGpsSensorGroup() {
        return gpsSensorGroup;
    }
}
