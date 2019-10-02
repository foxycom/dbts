package de.uni_passau.dbts.benchmark.workload.ingestion;

import de.uni_passau.dbts.benchmark.utils.Sensors;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;

import java.util.*;

/**
 * Batch of data points.
 */
public class Batch {

  /** The bike the data points belong to. */
  private Bike bike;

  /** The time range of the data points stored in the batch. */
  private long timeRange;

  /** Sensors and their data points. */
  private Map<Sensor, DataPoint[]> entries = new HashMap<>();

  /**
   * Creates an instance.
   *
   * @param bike A bike.
   */
  public Batch(Bike bike) {
    this.bike = bike;
  }

  /**
   * Returns the bike the data points belong to.
   *
   * @return Bike.
   */
  public Bike getBike() {
    return bike;
  }

  /**
   * Sets a new bike.
   *
   * @param bike New bike.
   */
  public void setBike(Bike bike) {
    this.bike = bike;
  }

  /**
   * Adds a sensor along with its data points.
   *
   * @param sensor Sensor.
   * @param values Sensor's values.
   */
  public void add(Sensor sensor, DataPoint[] values) {
    entries.put(sensor, values);
  }

  /**
   * Returns the number of points in the batch.
   *
   * @return Number of points.
   */
  public int pointNum() {
    int pointNum = 0;
    for (Sensor sensor : entries.keySet()) {
      pointNum += entries.get(sensor).length;
    }
    return pointNum;
  }

  /**
   * Sets a new time range.
   *
   * @param timeRange New time range.
   */
  public void setTimeRange(long timeRange) {
    this.timeRange = timeRange;
  }

  /**
   * Returns the time range of data points.
   *
   * @return Time range.
   */
  public long getTimeRange() {
    return this.timeRange;
  }

  /**
   * Returns all data points in the batch.
   *
   * @return Data points of the batch.
   */
  public Map<Sensor, DataPoint[]> getEntries() {
    return entries;
  }

  /**
   * Transforms the batch from column-oriented format (each column represents readings of one sensor
   * group) to row-oriented format.
   *
   * @return Transformed batch.
   */
  public Map<Long, List<String>> transform() {
    Map<Long, List<String>> rows = new TreeMap<>();
    List<Sensor> sensors = bike.getSensors();
    int columns = sensors.stream().mapToInt(sensor -> sensor.getFields().size()).sum();

    List<String> emptyRow = new ArrayList<>(columns);
    for (int column = 0; column < columns; column++) {
      emptyRow.add("NULL");
    }

    Sensor mostFrequentSensor = Sensors.minInterval(sensors);
    for (DataPoint dataPoint : entries.get(mostFrequentSensor)) {
      rows.computeIfAbsent(dataPoint.getTimestamp(), k -> new ArrayList<>(emptyRow));
    }

    int column = 0;
    for (Sensor sensor : sensors) {
      for (DataPoint dataPoint : entries.get(sensor)) {
        String[] values = dataPoint.getValues();
        int valueOffset = 0;
        for (String value : values) {
          rows.get(dataPoint.getTimestamp()).set(column + valueOffset, value);
          valueOffset++;
        }
      }
      column += sensor.getFields().size();
    }
    return rows;
  }
}
