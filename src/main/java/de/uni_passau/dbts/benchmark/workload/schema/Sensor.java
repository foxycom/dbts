package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.function.Function;
import de.uni_passau.dbts.benchmark.tsdb.DB;

import java.util.List;


public interface Sensor {

  /**
   * Returns the name of a sensor.
   *
   * @return Name of a sensor.
   */
  String getName();

  /**
   * Computes a timestamp based on the given <code>stepOffset</code>.
   *
   * @param stepOffset A global (in the scope of a bike) index of a sensor value.
   * @return Respective timestamp.
   */
  long getTimestamp(long stepOffset);

  /**
   * Returns a value of the first field.
   *
   * @param currentTimestamp Timestamp of the value.
   * @param currentDb The DBMS to benchmark in the current run.
   * @return Sensor value.
   */
  String getValue(long currentTimestamp, DB currentDb);

  /**
   * Returns values of all sensor fields.
   *
   * @param currentTimestamp Timestamp of the values.
   * @param currentDb The DBMS to benchmark in the current run.
   * @return Sensor values.
   */
  String[] getValues(long currentTimestamp, DB currentDb);

  /**
   * Checks if the sensor yields a value for the given sensor.
   *
   * @param currentTimestamp Timestamp to check a value against.
   * @return True if the sensor has a value, false otherwise.
   */
  boolean hasValue(long currentTimestamp);

  /**
   * Returns sampling interval of the sensor.
   *
   * @return Sampling interval.
   */
  long getInterval();

  /**
   * Sets sampling interval of the sensor to the given value.
   *
   * @param interval New sampling interval.
   */
  void setInterval(long interval);

  /**
   * Returns the data type of sensor's values.
   *
   * @return The data type.
   */
  String getDataType();

  /**
   * Sets a new data type.
   *
   * @param dataType New data type.
   */
  void setDataType(String dataType);

  /**
   * Sets a new sampling frequency.
   *
   * @param freq New sampling frequency.
   */
  void setFrequency(int freq);

  /**
   * Returns the name of the table this sensor's values are stored in. Can be used when each
   * sensor's / sensors group's time series are saved in a separate table.
   *
   * @return Name of the table.
   */
  String getTableName();

  /**
   * Returns the data function.
   *
   * @return Data function.
   */
  Function getFunction();

  /**
   * Returns the sensor group this sensor belongs to.
   *
   * @return Sensor group.
   */
  SensorGroup getSensorGroup();

  /**
   * Sets a new sensor group.
   *
   * @param sensorGroup New sensor group.
   */
  void setSensorGroup(SensorGroup sensorGroup);

  /**
   * Returns the list of fields of the sensor.
   *
   * @return List of fields.
   */
  List<String> getFields();

  /**
   * Sets a new tick.
   *
   * @param tick A new tick.
   */
  void setTick(long tick);
}
