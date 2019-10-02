package de.uni_passau.dbts.benchmark.workload.schema;

import java.util.ArrayList;
import java.util.List;

/** Contains similar sensors. The sensors should have the same data type and fields. */
public class SensorGroup {

  /** Name of the group. */
  private String name;

  /** List of sensors that belong to the group. */
  private List<Sensor> sensors = new ArrayList<>();

  /**
   * Creates a new group.
   *
   * @param sensors List of sensors.
   */
  public SensorGroup(List<Sensor> sensors) {
    this.sensors = sensors;
  }

  /**
   * Creates a new group.
   *
   * @param name Name of the group.
   */
  public SensorGroup(String name) {
    this.name = name;
  }

  /**
   * Returns the name of the group.
   *
   * @return Name of the group.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Adds a sensor to the group.
   *
   * @param sensor A new sensor.
   */
  public void addSensor(Sensor sensor) {
    sensors.add(sensor);
  }

  /**
   * Returns the data type of sensors' values.
   *
   * @return The data type.
   */
  public String getDataType() {
    return sensors.get(0).getDataType();
  }

  /**
   * Returns the name of the table the sensors are stored in. Should only be used in the narrow
   * table mode.
   *
   * @return Name of the table.
   */
  public String getTableName() {
    return name + "_benchmark";
  }

  /**
   * Returns the list of fields.
   *
   * @return List of fields.
   */
  public List<String> getFields() {
    return sensors.get(0).getFields();
  }

  /**
   * Returns the sensors of this group.
   *
   * @return List of sensors.
   */
  public List<Sensor> getSensors() {
    return sensors;
  }
}
