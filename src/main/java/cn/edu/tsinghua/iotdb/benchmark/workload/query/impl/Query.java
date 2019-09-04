package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class Query {
    private List<DeviceSchema> deviceSchemas;
    private SensorGroup sensorGroup;

    public Query(List<DeviceSchema> deviceSchemas) {
        this.deviceSchemas = deviceSchemas;
    }

    public Query(List<DeviceSchema> deviceSchemas, SensorGroup sensorGroup) {
        this.deviceSchemas = deviceSchemas;
        this.sensorGroup = sensorGroup;
    }

    public SensorGroup getSensorGroup() {
        return sensorGroup;
    }


    public List<DeviceSchema> getDeviceSchemas() {
        return deviceSchemas;
    }
}
