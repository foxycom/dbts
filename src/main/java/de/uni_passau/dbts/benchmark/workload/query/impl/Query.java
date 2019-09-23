package de.uni_passau.dbts.benchmark.workload.query.impl;

import de.uni_passau.dbts.benchmark.enums.Aggregation;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;
import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;
import de.uni_passau.dbts.benchmark.workload.schema.Sensor;
import java.util.List;

public class Query {
  private List<Bike> bikes;
  private Sensor sensor;
  private Sensor gpsSensor;
  private long startTimestamp;
  private long endTimestamp;
  private Aggregation aggrFunc;
  private double threshold;
  private GeoPoint location;

  public Query() {}

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

  public Sensor getSensor() {
    return sensor;
  }

  /**
   * Sets a sensor to use in the query.
   *
   * @param sensor Sensor to use.
   * @return The muted object to use in the builder style.
   */
  public Query setSensor(Sensor sensor) {
    this.sensor = sensor;
    return this;
  }

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

  public Aggregation getAggrFunc() {
    return aggrFunc;
  }

  public Query setAggrFunc(Aggregation func) {
    this.aggrFunc = func;
    return this;
  }

  public double getThreshold() {
    return threshold;
  }

  public Query setThreshold(double threshold) {
    this.threshold = threshold;
    return this;
  }

  /**
   * Returns the GPS location stored in the
   *
   * @return
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
