package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

public interface IWorkload {

  Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException;

  PreciseQuery getPreciseQuery() throws WorkloadException;

  RangeQuery getRangeQuery() throws WorkloadException;

  RangeQuery getGpsRangeQuery() throws WorkloadException;

  ValueRangeQuery getGpsValueRangeQuery() throws WorkloadException;

  ValueRangeQuery getValueRangeQuery() throws WorkloadException;

  AggRangeQuery getAggRangeQuery() throws WorkloadException;

  AggValueQuery getAggValueQuery() throws WorkloadException;

  AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException;

  GroupByQuery getGroupByQuery() throws WorkloadException;

  LatestPointQuery getLatestPointQuery() throws WorkloadException;

  HeatmapRangeQuery getHeatmapRangeQuery() throws WorkloadException;

}
