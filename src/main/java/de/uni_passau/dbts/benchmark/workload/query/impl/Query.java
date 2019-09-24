package de.uni_passau.dbts.benchmark.workload.query.impl;

import de.uni_passau.dbts.benchmark.enums.Aggregation;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import java.util.List;

/**
 * Universal object that stores query parameters for any benchmark scenario.
 */
public class Query {

  /** List of bikes to query. */
  private List<Bike> bikes;

  /** Sensor to query. */
  private Sensor sensor;

  /** GPS sensor to query. */
  private Sensor gpsSensor;

  /** Left boundary of a time range. */
  private long startTimestamp;

  /** Right boundary of a time range. */
  private long endTimestamp;

  /** Aggregation function. */
  private Aggregation aggrFunc;

  /** Value threshold. */
  private double threshold;

  /** GPS location. */
  private GeoPoint location;

  /**
   * Returns the bikes list.
   *
   * @return Bikes list.
   */
  public List<Bike> getBikes() {
    return bikes;
  }

  /**
   * Sets bikes to use in the query.
   *
   * @param bikes List of bikes.
   * @return The muted object to use in the builder style.
   */
  public Query setBikes(List<Bike> bikes) {
    this.bikes = bikes;
    return this;
  }

  /**
   * Returns the basic sensor.
   *
   * @return Basic sensor.
   */
  public Sensor getSensor() {
    return sensor;
  }

  /**
   * Sets a basic sensor to use in the query.
   *
   * @param sensor Basic sensor to use.
   * @return The muted object to use in the builder style.
   */
  public Query setSensor(Sensor sensor) {
    this.sensor = sensor;
    return this;
  }

  /**
   * Returns the GPS sensor.
   *
   * @return GPS sensor.
   */
  public Sensor getGpsSensor() {
    return gpsSensor;
  }

  /**
   * Sets a GPS sensor to use in the query.
   *
   * @param gpsSensor GPS sensor to use.
   * @return The muted object to use in the builder style.
   */
  public Query setGpsSensor(Sensor gpsSensor) {
    this.gpsSensor = gpsSensor;
    return this;
  }

  /**
   * Returns the left temporal boundary.
   *
   * @return Left temporal boundary.
   */
  public long getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * Sets the left temporal boundary of a time range.
   *
   * @param startTimestamp Left timestamp boundary.
   * @return The muted object to use in the builder style.
   */
  public Query setStartTimestamp(long startTimestamp) {
    this.startTimestamp = startTimestamp;
    return this;
  }

  /**
   * Returns the right temporal boundary.
   *
   * @return Right temporal boundary.
   */
  public long getEndTimestamp() {
    return endTimestamp;
  }

  /**
   * Sets the right temporal boundary of a time range.
   *
   * @param endTimestamp Right timestamp boundary.
   * @return The muted object to use in the builder style.
   */
  public Query setEndTimestamp(long endTimestamp) {
    this.endTimestamp = endTimestamp;
    return this;
  }

  /**
   * Returns the aggregation function of the query.
   *
   * @return Aggregation function.
   */
  public Aggregation getAggrFunc() {
    return aggrFunc;
  }

  /**
   * Sets an aggregation function, which is used to aggregate values of a sensor,
   * see {@link #setSensor(Sensor)}.
   *
   * @param func Aggregation function.
   * @return The muted object to use in the builder style.
   */
  public Query setAggrFunc(Aggregation func) {
    this.aggrFunc = func;
    return this;
  }

  /**
   * Returns the value threshold of the query.
   *
   * @return Value threshold.
   */
  public double getThreshold() {
    return threshold;
  }

  /**
   * Sets a value threshold to compare values of the sensor, see {@link #setSensor(Sensor)}.
   *
   * @param threshold Threshold value.
   * @return The muted object to use in the builder style.
   */
  public Query setThreshold(double threshold) {
    this.threshold = threshold;
    return this;
  }

  /**
   * Returns the GPS location stored in the
   *
   * @return GPS location.
   */
  public GeoPoint getLocation() {
    return location;
  }

  /**
   * Sets a GPS location to use in the query.
   *
   * @param location GPS location.
   * @return The muted object to use in the builder style.
   */
  public Query setLocation(GeoPoint location) {
    this.location = location;
    return this;
  }
}
