package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;

import java.util.List;

/**
 * A set of methods that each database complies with, in order to insert and erase data, and
 * execute scenarios.
 *
 * TODO an object with scenarios should probably be injected into database wrappers instead, in
 * order to prevent changing the interface when new scenario is added
 */
public interface Database {

  /**
   * Initializes a connection to the database. There is one DB instance per client thread.
   */
  void init() throws TsdbException;

  /**
   * Erases previous state of a database. Called once upon a benchmark
   * start, when <code>erase</code> is set to <code>true</code> in the config file.
   */
  void cleanup() throws TsdbException;

  /** Closes the connection to a database. Called once per DB instance. */
  void close() throws TsdbException;

  /**
   * Creates a formal schema if needed in a database. Called once upon a benchmark start, when
   * <code>createSchema</code> is set to <code>true</code> in the config file.
   *
   * @param schemaList schema of devices to register
   */
  void registerSchema(List<Bike> schemaList) throws TsdbException;

  /**
   * Returns the size of the database.
   *
   * @return The size of the database in GB.
   */
  float getSize() throws TsdbException;

  /**
   * Insert one batch into a database. The DB implementation needs to resolve the data in batch
   * which contains device schema and sensor readings.
   *
   * @param batch A batch of sensor readings for a single bike.
   * @return Status of the execution.
   */
  Status insertOneBatch(Batch batch);

  /**
   * Selects one data point using a precise timestamp.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status precisePoint(Query query);

  /**
   * Selects GPS data for a bike within a certain time range.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status gpsPathScan(Query query);

  /**
   * Selects sensor data of a bike within a time range with current sensor values exceeding
   * a certain threshold.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status identifyTrips(Query query);

  /**
   * Selects distinct bikes that did not any data since a certain timestamp.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status offlineBikes(Query query);

  /**
   * Selects last timestamp for each bike with average current sensor values per minute exceeding a
   * certain threshold.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status lastTimeActivelyDriven(Query query);

  /**
   * Downsamples data of a bike within a time range to 1 minute buckets.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status downsample(Query query);

  /**
   * Selects last GPS locations of each bike.
   *
   * @param query Query params object.
   * @return status Status of the execution.
   */
  Status lastKnownPosition(Query query);

  /**
   * Computes an average of particles sensor values for each stored location.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status airPollutionHeatMap(Query query);

  /**
   * Selects data of a bike within a time range with average current sensor values exceeding
   * a certain threshold and computes the sum of distances between each GPS location in the result
   * set.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status distanceDriven(Query query);

  /**
   * Selects bikes whose last known GPS locations lie within a certain area.
   *
   * @param query Query params object.
   * @return Status of the execution.
   */
  Status bikesInLocation(Query query);
}
