package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;

import java.util.List;

public interface IDatabase {

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per
   * client thread.
   */
  void init() throws TsdbException;

  /**
   * Cleanup any state for this DB, including the old data deletion. Called once before each test if
   * IS_DELETE_DATA=true.
   */
  void cleanup() throws TsdbException;

  /** Close the DB instance connections. Called once per DB instance. */
  void close() throws TsdbException;

  /**
   * Called once before each test if CREATE_SCHEMA=true.
   *
   * @param schemaList schema of devices to register
   */
  void registerSchema(List<Bike> schemaList) throws TsdbException;

  /**
   * Returns the size of the benchmarked data.
   *
   * @return The size of the data in GB.
   */
  float getSize() throws TsdbException;

  /**
   * Insert one batch into the database, the DB implementation needs to resolve the data in batch
   * which contains device schema and Map[Long, List[String]] records. The key of records is a
   * timestamp and the value is a list of sensor value data.
   *
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status insertOneBatch(Batch batch);

  /**
   * Query data of one or multiple sensors at a precise timestamp. e.g. select v1... from data where
   * time = ? and device in ?
   *
   * @param query universal precise query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status precisePoint(Query query);

  /**
   * TODO comment
   *
   * @return
   */
  Status gpsPathScan(Query query);

  /**
   * TODO comment
   *
   * @param query
   * @return
   */
  Status identifyTrips(Query query);

  /**
   * TODO comment
   *
   * @param query
   * @return
   */
  Status offlineBikes(Query query);

  /**
   * Query data of one or multiple sensors in a time range with a value filter. e.g. select v1...
   * from data where time >= ? and time <= ? and v1 > ? and device in ?
   *
   * @param query contains universal range query with value filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status lastTimeActivelyDriven(Query query);

  /**
   * Query aggregated group-by-time data of one or multiple sensors within a time range. e.g. SELECT
   * max(s_0), max(s_1) FROM group_0, group_1 WHERE ( device = ’d_3’ OR device = ’d_8’) AND time >=
   * 2010-01-01 12:00:00 AND time <= 2010-01-01 12:10:00 GROUP BY time(60000ms)
   *
   * @param query contains universal group by query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status downsample(Query query);

  /**
   * Query the latest(max-timestamp) data of one or multiple sensors. e.g. select time, v1... where
   * device = ? and time = max(time)
   *
   * @param query contains universal latest point query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status lastKnownPosition(Query query);

  /**
   * TODO comment
   *
   * @param query
   * @return
   */
  Status airPollutionHeatMap(Query query);

  /**
   * TODO comment
   *
   * @param query
   * @return
   */
  Status distanceDriven(Query query);

  /**
   * TODO comment
   *
   * @param query
   * @return
   */
  Status bikesInLocation(Query query);
}
