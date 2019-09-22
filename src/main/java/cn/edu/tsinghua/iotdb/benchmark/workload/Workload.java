package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;

public interface Workload {

  Batch getOneBatch(Bike bike, long loopIndex) throws WorkloadException;

  Query getQuery(String sensorType) throws WorkloadException;
}
