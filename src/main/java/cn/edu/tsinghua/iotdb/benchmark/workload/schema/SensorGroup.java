package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import java.util.ArrayList;
import java.util.List;

public class SensorGroup {
    private String name;
    private List<Sensor> sensors = new ArrayList<>();

    public SensorGroup(List<Sensor> sensors) {
        this.sensors = sensors;
    }

    public SensorGroup(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void addSensor(Sensor sensor) {
        sensors.add(sensor);
    }

    public String getDataType() {
        return sensors.get(0).getDataType();
    }

    public String getTableName() {
        return name + "_benchmark";
    }

    public List<String> getFields() {
        return sensors.get(0).getFields();
    }

    public List<Sensor> getSensors() {
        return sensors;
    }
}
