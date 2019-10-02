package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.function.Function;
import de.uni_passau.dbts.benchmark.tsdb.DB;

import java.math.RoundingMode;
import java.text.NumberFormat;
import java.util.*;

import static de.uni_passau.dbts.benchmark.conf.Constants.START_TIMESTAMP;

/**
 * A very basic sensor type that only yields primitive values.
 */
public class BasicSensor implements Sensor {
  private static NumberFormat nf = NumberFormat.getNumberInstance(Locale.US);

  /** Values function. */
  private Function function;

  /** Name of the sensor. */
  private String name;

  /** The sensor group the sensor belongs to. */
  private SensorGroup sensorGroup;

  /** Sampling interval. */
  long interval;

  /** Values' data type. */
  private String dataType;

  /** Value fields. */
  private List<String> fields;

  /**
   * Creates a new copy.
   *
   * @param other The other sensor to copy.
   */
  public BasicSensor(BasicSensor other) {
    this.name = other.name;
    this.function = other.function;
    this.sensorGroup = other.sensorGroup;
    this.interval = other.interval;
    this.dataType = other.dataType;
    this.fields = new ArrayList<>(other.fields.size());
    this.fields.addAll(other.fields);
  }

  /**
   * Creates a new sensor instance.
   *
   * @param name Name of the sensor.
   * @param sensorGroup The sensor group the sensor should belong to
   * @param function Values function.
   * @param freq Sampling frequency.
   * @param dataType Values' data type.
   * @param fields Values fields.
   */
  public BasicSensor(
      String name,
      SensorGroup sensorGroup,
      Function function,
      int freq,
      String dataType,
      List<String> fields) {
    nf.setRoundingMode(RoundingMode.HALF_UP);
    nf.setMaximumFractionDigits(2);
    nf.setMinimumFractionDigits(2);
    nf.setGroupingUsed(false);

    this.name = name;
    this.sensorGroup = sensorGroup;
    this.function = function;
    this.interval = 1000 / freq; // in ms
    this.dataType = dataType;
    if (fields == null) {
      fields = new ArrayList<>(1);
      fields.add("value");
    }
    this.fields = fields;
  }

  /**
   * Creates an instance of sensor.
   *
   * @param name The name of the sensor.
   * @param sensorGroup The sensor group which the sensor belongs to.
   * @param function The parameters of data function.
   * @param freq The frequency the sensor samples its data (in Hz).
   * @param dataType The data type of values.
   */
  public BasicSensor(
      String name, SensorGroup sensorGroup, Function function, int freq, String dataType) {
    this(name, sensorGroup, function, freq, dataType, null);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getTimestamp(long stepOffset) {
    return START_TIMESTAMP + interval * stepOffset;
  }

  @Override
  public String getValue(long currentTimestamp, DB currentDb) {
    Number value = function.get(currentTimestamp);
    return nf.format(value);
  }

  @Override
  public String[] getValues(long currentTimestamp, DB currentDb) {
    String[] values = new String[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      values[i] = nf.format(function.get(currentTimestamp));
    }
    return values;
  }

  public boolean hasValue(long currentTimestamp) {
    // TODO implement
    return true;
  }

  @Override
  public long getInterval() {
    return interval;
  }

  @Override
  public void setInterval(long interval) {
    this.interval = interval;
  }

  @Override
  public String getDataType() {
    return dataType;
  }

  @Override
  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  @Override
  public void setFrequency(int frequency) {
    this.interval = 1000 / frequency;
  }

  @Override
  public String getTableName() {
    return sensorGroup.getTableName();
  }

  @Override
  public Function getFunction() {
    return function;
  }

  @Override
  public SensorGroup getSensorGroup() {
    return sensorGroup;
  }

  @Override
  public void setSensorGroup(SensorGroup sensorGroup) {
    this.sensorGroup = sensorGroup;
  }

  public List<String> getFields() {
    return fields;
  }

  @Override
  public void setTick(long tick) {
    // Not needed.
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Sensor sensor = (Sensor) o;
    return interval == sensor.getInterval() && name.equals(sensor.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, function, interval);
  }
}
