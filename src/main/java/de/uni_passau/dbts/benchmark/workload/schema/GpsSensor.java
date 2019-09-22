package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.function.GeoFunction;
import de.uni_passau.dbts.benchmark.tsdb.DB;

import java.util.List;

public class GpsSensor extends BasicSensor {
  private GeoFunction function;
  private GeoPoint lastLocation;
  private long tick = -1;

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

  public GpsSensor(
      String name, SensorGroup sensorGroup, GeoFunction function, int freq, String dataType) {
    super(name, sensorGroup, null, freq, dataType);
    this.function = function;
  }

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
