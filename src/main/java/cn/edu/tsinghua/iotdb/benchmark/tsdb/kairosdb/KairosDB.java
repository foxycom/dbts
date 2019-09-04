package cn.edu.tsinghua.iotdb.benchmark.tsdb.kairosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.model.KairosDataModel;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Aggregator;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.AggregatorFactory.FilterOperation;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KairosDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(KairosDB.class);
  private String writeUrl;
  private HttpClient client;
  private Config config;

  private static final String GROUP_STR = "group";
  private static final String DEVICE_STR = "device";

  public KairosDB() {
    config = ConfigParser.INSTANCE.config();
    writeUrl = config.DB_URL + "/api/v1/datapoints";

  }

  @Override
  public void init() throws TsdbException {
    try {
      client = new HttpClient(config.DB_URL);
    } catch (MalformedURLException e) {
      throw new TsdbException(
          "Init KairosDB client failed, the url is " + config.DB_URL + ". Message is " + e
              .getMessage());
    }
  }

  @Override
  public void cleanup() {
    try {
      for (String metric : client.getMetricNames()) {
        // skip kairosdb internal info metrics
        if(metric.contains("kairosdb.")){
          continue;
        }
        client.deleteMetric(metric);
      }
      // wait for deletion complete
      LOGGER.info("[KAIROSDB]:Waiting {}ms for old data deletion.", config.ERASE_WAIT_TIME);
      Thread.sleep(config.ERASE_WAIT_TIME);
    } catch (Exception e) {
      LOGGER.error("Delete old data failed because ", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    try {
      client.close();
    } catch (IOException e) {
      throw new TsdbException("Close KairosDB client failed, because " + e.getMessage());
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) {
    //no need for KairosDB
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }


  private LinkedList<KairosDataModel> createDataModel(DeviceSchema deviceSchema, long timestamp,
      List<String> recordValues) {
    LinkedList<KairosDataModel> models = new LinkedList<>();
    String groupId = deviceSchema.getGroup();
    int i = 0;
    for (Sensor sensor : deviceSchema.getSensors()) {
      KairosDataModel model = new KairosDataModel();
      model.setName(sensor.getName());
      // KairosDB do not support float as data type
      if (config.DATA_TYPE.equalsIgnoreCase("FLOAT")) {
        model.setType("double");
      } else {
        model.setType(config.DATA_TYPE.toLowerCase());
      }
      model.setTimestamp(timestamp);
      model.setValue(recordValues.get(i));
      Map<String, String> tags = new HashMap<>();
      tags.put(GROUP_STR, groupId);
      tags.put(DEVICE_STR, deviceSchema.getDevice());
      model.setTags(tags);
      models.addLast(model);
      i++;
    }
    return models;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    LinkedList<KairosDataModel> models = new LinkedList<>();
    for (Record record : batch.getRecords()) {
      models.addAll(createDataModel(batch.getDeviceSchema(), record.getTimestamp(),
          record.getRecordDataValue()));
    }
    String body = JSON.toJSONString(models);
    LOGGER.debug("body: {}", body);
    try {
      st = System.nanoTime();
      String response = HttpRequest.sendPost(writeUrl, body);
      en = System.nanoTime();
      LOGGER.debug("response: {}", response);
      return new Status(true, en - st);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    long time = preciseQuery.getTimestamp();
    QueryBuilder builder = constructBuilder(time, time, preciseQuery.getDeviceSchema());
    return executeOneQuery(builder);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    long startTime = rangeQuery.getStartTimestamp();
    long endTime = rangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, rangeQuery.getDeviceSchema());
    return executeOneQuery(builder);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    long startTime = valueRangeQuery.getStartTimestamp();
    long endTime = valueRangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, valueRangeQuery.getDeviceSchema());
    Aggregator filterAggre = AggregatorFactory
        .createFilterAggregator(FilterOperation.LTE, valueRangeQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre);
    return executeOneQuery(builder);
  }

  @Override
  public Status gpsRangeQuery(RangeQuery RangeQuery) {
    return null;
  }

  @Override
  public Status gpsValueRangeQuery(GpsValueRangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status gpsAggValueRangeQuery(GpsAggValueRangeQuery gpsAggValueRangeQuery) {
    return null;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    /*long startTime = aggRangeQuery.getStartTimestamp();
    long endTime = aggRangeQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, aggRangeQuery.getDeviceSchema());
    // convert to second
    int timeInterval = (int) (endTime - startTime) + 1;
    Aggregator aggregator = new SamplingAggregator(aggRangeQuery.getAggrFunc(), timeInterval,
        TimeUnit.MILLISECONDS);
    addAggreForQuery(builder, aggregator);
    return executeOneQuery(builder);*/
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    /*long startTime = aggValueQuery.getStartTimestamp();
    long endTime = aggValueQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, aggValueQuery.getDeviceSchema());
    Aggregator funAggre = new SamplingAggregator(aggValueQuery.getAggrFunc(), 5000, TimeUnit.YEARS);
    Aggregator filterAggre = AggregatorFactory
        .createFilterAggregator(FilterOperation.LTE, aggValueQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre, funAggre);
    return executeOneQuery(builder);*/
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    /*long startTime = aggRangeValueQuery.getStartTimestamp();
    long endTime = aggRangeValueQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime,
        aggRangeValueQuery.getDeviceSchema());
    int timeInterval = (int) (endTime - startTime) + 1;
    Aggregator funAggre = new SamplingAggregator(aggRangeValueQuery.getAggrFunc(), timeInterval,
        TimeUnit.SECONDS);
    Aggregator filterAggre = AggregatorFactory
        .createFilterAggregator(FilterOperation.LTE, aggRangeValueQuery.getValueThreshold());
    addAggreForQuery(builder, filterAggre, funAggre);
    return executeOneQuery(builder);*/
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    /*long startTime = groupByQuery.getStartTimestamp();
    long endTime = groupByQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, groupByQuery.getDeviceSchema());
    Aggregator funAggre = new SamplingAggregator(groupByQuery.getAggrFunc(),
        (int) groupByQuery.getGranularity(), TimeUnit.MILLISECONDS);
    addAggreForQuery(builder, funAggre);
    return executeOneQuery(builder);*/
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    //latestPointQuery
    long startTime = latestPointQuery.getStartTimestamp();
    long endTime = latestPointQuery.getEndTimestamp();
    QueryBuilder builder = constructBuilder(startTime, endTime, latestPointQuery.getDeviceSchema());
    Aggregator aggregator = AggregatorFactory.createLastAggregator(5000, TimeUnit.YEARS);
    addAggreForQuery(builder, aggregator);
    return executeOneQuery(builder);
  }

  @Override
  public Status heatmapRangeQuery(GpsValueRangeQuery gpsRangeQuery) {
    return null;
  }

  @Override
  public Status distanceRangeQuery(GpsValueRangeQuery gpsRangeQuery) {
    return null;
  }

  @Override
  public Status bikesInLocationQuery(GpsRangeQuery gpsRangeQuery) {
    return null;
  }

  private Status executeOneQuery(QueryBuilder builder) {
    LOGGER.info("[JSON] {}", builder.build());
    int queryResultPointNum = 0;
    try {
      long st = System.nanoTime();
      QueryResponse response = client.query(builder);
      long en = System.nanoTime();
      for (QueryResult query : response.getQueries()) {
        for (Result result : query.getResults()) {
          queryResultPointNum += result.getDataPoints().size();
        }
      }
      return new Status(true, en - st, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, 0, 0, e, builder.toString());
    }
  }

  private QueryBuilder constructBuilder(long st, long et, List<DeviceSchema> deviceSchemaList) {
    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(new Date(st))
        .setEnd(new Date(et));
    for (DeviceSchema deviceSchema : deviceSchemaList) {
      for (Sensor sensor : deviceSchema.getSensors()) {
        builder.addMetric(sensor.getName())
            .addTag(DEVICE_STR, deviceSchema.getDevice())
            .addTag(GROUP_STR, deviceSchema.getGroup());
      }
    }
    return builder;
  }

  private void addAggreForQuery(QueryBuilder builder, Aggregator... aggregatorArray) {
    builder.getMetrics().forEach(queryMetric -> {
      for (Aggregator aggregator : aggregatorArray) {
        queryMetric.addAggregator(aggregator);
      }
    });
  }
}
