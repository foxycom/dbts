package de.uni_passau.dbts.benchmark.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;

import java.util.List;

/**
 * Sensors utility.
 */
public class Sensors {

  /**
   * Config singleton.
   */
  private static Config config = ConfigParser.INSTANCE.config();

  /**
   * Finds the sensor with the lowest sampling interval out of the list.
   *
   * @param sensors List of sensors.
   * @return Sensor with the lowest sampling interval.
   */
  public static Sensor minInterval(List<Sensor> sensors) {
    checkNotNull(sensors);
    checkArgument(!sensors.isEmpty());

    Sensor sensorWithMinInterval = sensors.get(0);
    for (Sensor sensor : sensors) {
      if (sensor.getInterval() < sensorWithMinInterval.getInterval()) {
        sensorWithMinInterval = sensor;
      }
    }
    return sensorWithMinInterval;
  }

  /**
   * Returns a sensor the given type.
   *
   * @param type The type of sensor.
   * @return A sensor of the given type.
   */
  public static Sensor ofType(String type) {
    checkNotNull(type);

    Sensor typedSensor = null;
    for (Sensor sensor : config.SENSORS) {
      if (sensor.getSensorGroup().getName().toLowerCase().contains(type.toLowerCase())) {
        typedSensor = sensor;
        break;
      }
    }
    return typedSensor;
  }

  /**
   * Returns a sensor group of the given type.
   *
   * @param groupType The type name.
   * @return Sensor group.
   */
  public static SensorGroup groupOfType(String groupType) {
    SensorGroup sensorGroup = null;
    for (SensorGroup sg : config.SENSOR_GROUPS) {
      if (sg.getName().toLowerCase().contains(groupType.toLowerCase())) {
        sensorGroup = sg;
        break;
      }
    }
    return sensorGroup;
  }
}
