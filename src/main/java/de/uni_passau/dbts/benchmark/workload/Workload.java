package de.uni_passau.dbts.benchmark.workload;

import de.uni_passau.dbts.benchmark.workload.ingestion.Batch;
import de.uni_passau.dbts.benchmark.workload.query.impl.Query;
import de.uni_passau.dbts.benchmark.workload.schema.Bike;

public interface Workload {

  Batch getOneBatch(Bike bike, long loopIndex) throws WorkloadException;

  Query getQuery(String sensorType) throws WorkloadException;
}
