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
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Bike;
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
import org.kairosdb.client.builder.QueryBuilder;
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
  public void registerSchema(List<Bike> schemaList) {
    //no need for KairosDB
  }

  @Override
  public float getSize() throws TsdbException {
    return 0;
  }


  private LinkedList<KairosDataModel> createDataModel(Bike bike, long timestamp,
                                                      List<String> recordValues) {
    LinkedList<KairosDataModel> models = new LinkedList<>();
    String groupId = bike.getGroup();
    int i = 0;
    for (Sensor sensor : bike.getSensors()) {
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
      tags.put(DEVICE_STR, bike.getName());
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
      models.addAll(createDataModel(batch.getBike(), record.getTimestamp(),
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
  public Status precisePoint(Query query) {
    return null;
  }

  @Override
  public Status gpsPathScan(Query query) {
    return null;
  }

  @Override
  public Status identifyTrips(Query query) {
    return null;
  }

  @Override
  public Status dangerousSpots(Query query) {
    return null;
  }

  @Override
  public Status lastTimeActivelyDriven(Query query) {
    return null;
  }

  @Override
  public Status downsample(Query query) {
    return null;
  }

  @Override
  public Status lastKnownPosition(Query query) {
    return null;
  }

  @Override
  public Status airQualityHeatMap(Query query) {
    return null;
  }

  @Override
  public Status distanceDriven(Query query) {
    return null;
  }

  @Override
  public Status bikesInLocation(Query query) {
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

  private QueryBuilder constructBuilder(long st, long et, List<Bike> bikeList) {
    QueryBuilder builder = QueryBuilder.getInstance();
    builder.setStart(new Date(st))
        .setEnd(new Date(et));
    for (Bike bike : bikeList) {
      for (Sensor sensor : bike.getSensors()) {
        builder.addMetric(sensor.getName())
            .addTag(DEVICE_STR, bike.getName())
            .addTag(GROUP_STR, bike.getGroup());
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
