package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.function.GeoFunction;
import de.uni_passau.dbts.benchmark.tsdb.DB;

import java.util.List;

/**
 * Sensor that yields geographic positions.
 */
public class GpsSensor extends BasicSensor {

  /** Geographic values function. */
  private GeoFunction function;

  /** Recently saved location. */
  private GeoPoint lastLocation;

  /** The timestmap of the next sampling tick. */
  private long tick = -1;

  /**
   * Creates a new instance.
   *
   * @param name Name of the sensor.
   * @param sensorGroup A sensor group.
   * @param function A values function.
   * @param freq Sampling frequency.
   * @param dataType Values' data type.
   * @param fields Fields.
   */
  public GpsSensor(
      String name,
      SensorGroup sensorGroup,
      GeoFunction function,
      int freq,
      String dataType,
      List<String> fields) {
    super(name, sensorGroup, null, freq, dataType, fields);
    this.function = function;
  }

  /**
   * Creates a new instance.
   *
   * @param name Name of the sensor.
   * @param sensorGroup A sensor group.
   * @param function A values function.
   * @param freq Sampling frequency.
   * @param dataType Values' data type.
   */
  public GpsSensor(
      String name, SensorGroup sensorGroup, GeoFunction function, int freq, String dataType) {
    super(name, sensorGroup, null, freq, dataType);
    this.function = function;
  }

  /**
   * Creates a new copy.
   *
   * @param other The other sensor to copy.
   * @param index Geo-function seed.
   */
  public GpsSensor(GpsSensor other, int index) {
    super(other);
    this.function = new GeoFunction(index);
    this.lastLocation = other.lastLocation;
  }

  @Override
  public String getValue(long currentTimestamp, DB currentDb) {
    if (tick < 0 || currentTimestamp - tick >= interval) {
      tick = currentTimestamp;
      lastLocation = function.get(Constants.SPAWN_POINT);
      return lastLocation.getValue(currentDb);
    } else {
      return lastLocation.getValue(currentDb);
    }
  }

  @Override
  public String[] getValues(long currentTimestamp, DB currentDb) {
    if (tick < 0 || currentTimestamp - tick >= interval) {
      tick = currentTimestamp;
      lastLocation = function.get(Constants.SPAWN_POINT);
      return lastLocation.getValue(currentDb).split(",");
    } else {
      return lastLocation.getValue(currentDb).split(",");
    }
  }

  @Override
  public void setTick(long tick) {
    this.tick = tick;
  }
}
