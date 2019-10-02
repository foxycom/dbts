package de.uni_passau.dbts.benchmark.workload;

import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;

/**
 * A unified interface for all workload types.
 */
public interface Workload {

  /**
   * Generates one batch of data points.
   *
   * @param bike The bike the batch of points should belong to.
   * @param loopIndex Loop index.
   * @return A batch of data points.
   * @throws WorkloadException if an error occurs while generating a batch.
   */
  Batch getOneBatch(Bike bike, long loopIndex) throws WorkloadException;

  /**
   * Generates query parameters.
   *
   * @param sensorType The sensor type to use in a query.
   * @return A query parameters object.
   * @throws WorkloadException if an error occurs while generating a query meta-object.
   */
  Query getQuery(String sensorType) throws WorkloadException;
}
