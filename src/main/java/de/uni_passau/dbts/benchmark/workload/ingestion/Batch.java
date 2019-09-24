package de.uni_passau.dbts.benchmark.workload.ingestion;

import de.uni_passau.dbts.benchmark.utils.Sensors;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;

import java.util.*;

public class Batch {

  private Bike bike;
  private long timeRange;
  private Map<Sensor, Point[]> entries = new HashMap<>();

  public Batch(Bike bike) {
    this.bike = bike;
  }

  public Bike getBike() {
    return bike;
  }

  public void setBike(Bike bike) {
    this.bike = bike;
  }

  public void add(Sensor sensor, Point[] values) {
    entries.put(sensor, values);
  }

  /**
   * use the row protocol which means data are organized in List[timestamp, List[value]]
   *
   * @return data point number in this batch
   */
  public int pointNum() {
    int pointNum = 0;
    for (Sensor sensor : entries.keySet()) {
      pointNum += entries.get(sensor).length;
    }
    return pointNum;
  }

  public void setTimeRange(long timeRange) {
    this.timeRange = timeRange;
  }

  public long getTimeRange() {
    return this.timeRange;
  }

  public Map<Sensor, Point[]> getEntries() {
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
    for (Point point : entries.get(mostFrequentSensor)) {
      rows.computeIfAbsent(point.getTimestamp(), k -> new ArrayList<>(emptyRow));
    }

    int column = 0;
    for (Sensor sensor : sensors) {
      for (Point point : entries.get(sensor)) {
        String[] values = point.getValues();
        int valueOffset = 0;
        for (String value : values) {
          rows.get(point.getTimestamp()).set(column + valueOffset, value);
          valueOffset++;
        }
      }
      column += sensor.getFields().size();
    }
    return rows;
  }
}
