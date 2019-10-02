package de.uni_passau.dbts.benchmark.workload.ingestion;

/**
 * This class represents a data point value, that is, a single sensor's reading. It carries a
 * timestamp and value, or multiple value in case the corresponding sensor was configured to
 * yield several values at once.
 */
public class DataPoint {
  private long timestamp;
  private String[] values;

  /**
   * Creates a new instance.
   *
   * @param timestamp Timestamp.
   * @param value Sensor value.
   */
  public DataPoint(long timestamp, String value) {
    this.timestamp = timestamp;
    this.values = new String[] {value};
  }

  /**
   * Creates a new instance with multiple values.
   *
   * @param timestamp Timestamp.
   * @param values Sensor values.
   */
  public DataPoint(long timestamp, String... values) {
    this.timestamp = timestamp;
    this.values = values;
  }

  /**
   * Returns the timestamp of the reading.
   *
   * @return Timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Returns the value of the reading.
   *
   * @return Value.
   */
  public String getValue() {
    return values[0];
  }

  /**
   * Returns the values of the readings.
   *
   * @return Values.
   */
  public String[] getValues() {
    return values;
  }

  /**
   * Checks if the data point contains multiple values.
   *
   * @return true if the data point contains multiple values, false otherwise.
   */
  public boolean hasMultipleValues() {
    return values.length > 1;
  }
}
