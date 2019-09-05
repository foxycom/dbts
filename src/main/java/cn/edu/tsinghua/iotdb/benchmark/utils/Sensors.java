package cn.edu.tsinghua.iotdb.benchmark.utils;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;

import java.util.List;

public class Sensors {
    private static Config config = ConfigParser.INSTANCE.config();

    public static Sensor minInterval(List<Sensor> sensors) {
        if (sensors.isEmpty()) {
            return null;
        }

        Sensor sensorWithMinInterval = sensors.get(0);
        for (Sensor sensor : sensors) {
            if (sensor.getInterval() < sensorWithMinInterval.getInterval()) {
                sensorWithMinInterval = sensor;
            }
        }
        return sensorWithMinInterval;
    }

    public static SensorGroup groupOfType(String type) {
        SensorGroup sensorGroup = null;
        for (SensorGroup sg : config.SENSOR_GROUPS) {
            if (sg.getName().contains(type)) {
                sensorGroup = sg;
                break;
            }
        }
        return sensorGroup;
    }
}