package cn.edu.tsinghua.iotdb.benchmark.db.kairosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.TSDB;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.KairosDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KairosDB extends TSDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(KairosDB.class);
    private String Url;
    private String queryUrl;
    private String writeUrl;
    private String deleteUrl;
    private String dataType = "double";
    private Config config;
    private MySqlLog mySql;
    private long labID;
    private float nano2million = 1000000;
    private Map<String, LinkedList<KairosDataModel>> dataMap = new HashMap<>();
    private Random sensorRandom;
    private static final String QUERY_START_TIME = "start_absolute";
    private static final String QUERY_END_TIME = "end_absolute";
    private static final String METRICS = "metrics";
    private static final String NAME = "name";
    private static final String AGGREGATORS = "aggregators";
    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private ProbTool probTool;
    private Random timestampRandom;
    private SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);

    public KairosDB(long labID) {
        config = ConfigDescriptor.getInstance().getConfig();
        mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        this.labID = labID;
        sensorRandom = new Random(1 + config.QUERY_SEED);
        probTool = new ProbTool();
        timestampRandom = new Random(2 + config.QUERY_SEED);
        Url = config.DB_URL;
        queryUrl = Url + "/api/v1/datapoints/query";
        writeUrl = Url + "/api/v1/datapoints";
        deleteUrl = Url + "/api/v1/metric/%s";
        mySql.initMysql(false);
    }


    @Override
    public void init() {
        //delete old data
        for (String sensor : config.SENSOR_CODES) {
            try {
                HttpRequest.sendDelete(String.format(deleteUrl, sensor), "");
            } catch (IOException e) {
                LOGGER.error("Delete metric {} failed when initializing KairosDB.", sensor);
                e.printStackTrace();
            }
        }
        // wait for deletion complete
        try {
            LOGGER.info("Waiting {}ms for old data deletion.", config.ERASE_WAIT_TIME);
            Thread.sleep(config.ERASE_WAIT_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createSchema() throws SQLException {
        //no need for KairosDB
    }

    @Override
    public long getLabID() {
        return labID;
    }

    @Override
    public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
        LinkedList<String> keys = new LinkedList<>();
        for (int i = 0; i < config.BATCH_SIZE; i++) {
            String key = UUID.randomUUID().toString();
            dataMap.put(key, createDataModel(batchIndex, i, device));
            keys.add(key);
        }
        insertOneBatch(keys, batchIndex, totalTime, errorCount, latencies);
    }

    private String getGroup(String device) {
        int deviceNum = getDeviceNum(device);
        int groupSize = config.DEVICES_NUMBER / config.DEVICE_GROUPS_NUMBER;
        int groupNum = deviceNum / groupSize;
        return "group_" + groupNum;
    }

    private LinkedList<KairosDataModel> createDataModel(int batchIndex, int dataIndex, String device) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        String groupId = getGroup(device);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            long currentTime = Constants.START_TIMESTAMP
                    + config.POINT_STEP * (batchIndex * config.BATCH_SIZE + dataIndex);
            if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
                currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
            }
            Number value = Function.getValueByFunctionIdAndParam(param, currentTime);
            KairosDataModel model = new KairosDataModel();
            model.setName(sensor);
            // KairosDB do not support float as data type
            model.setType(config.DATA_TYPE.toLowerCase());
            model.setTimestamp(currentTime);
            model.setValue(value);
            Map<String, String> tags = new HashMap<>();
            tags.put("group", groupId);
            tags.put("device", device);
            model.setTags(tags);
            models.addLast(model);
        }
        return models;
    }

    private int getDeviceNum(String device) {
        String[] parts = device.split("_");
        try {
            return Integer.parseInt(parts[1]);
        } catch (Exception e) {
            LOGGER.error("{} {}", device, e.getMessage());
            throw e;
        }
    }

    @Override
    public void insertOneBatch(LinkedList<String> keys, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {
        long startTime = 0, endTime = 0, latency = 0;
        String response = null;
        LinkedList<KairosDataModel> models = new LinkedList<>();
        for (String key : keys) {
            models.addAll(dataMap.get(key));
            dataMap.remove(key);
        }
        String body = JSON.toJSONString(models);
        LOGGER.debug(body);
        try {
            startTime = System.nanoTime();
            response = HttpRequest.sendPost(writeUrl, body);
            endTime = System.nanoTime();
            latency = endTime - startTime;
            latencies.add(latency);
            LOGGER.debug("response: " + response);
            LOGGER.info("{} execute ,{}, batch, it costs ,{},s, totalTime ,{},s, throughput ,{}, point/s",
                    Thread.currentThread().getName(), batchIndex, latency / 1000000000.0,
                    ((totalTime.get() + latency) / 1000000000.0),
                    (models.size() / (double) latency) * 1000000000);
            totalTime.set(totalTime.get() + latency);
            mySql.saveInsertProcess(batchIndex, latency / 1000000000.0, totalTime.get() / 1000000000.0, 0,
                    config.REMARK);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + models.size());
            LOGGER.error("Batch insert failed, the failed num is ,{}, Error：{}", models.size(), e.getMessage());
            mySql.saveInsertProcess(batchIndex, latency / 1000000000.0, totalTime.get() / 1000000000.0, models.size(),
                    config.REMARK + e.getMessage());
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public long getTotalTimeInterval() throws SQLException {
        //FIXME unchecked method
        Map<String, Object> queryMap = new HashMap<>();

        //queryMap.put("time_zone", "Etc/GMT+8");
        //queryMap.put("cache_time", 0);
        queryMap.put(QUERY_START_TIME, Constants.START_TIMESTAMP);
        List<Map<String, Object>> list = new ArrayList<>();


        Map<String, Object> subQuery = new HashMap<String, Object>();
        subQuery.put(NAME, "s_0");
        Map<String, List<String>> tags = new HashMap<>();
        List<String> deviceList = new ArrayList<>();
        List<String> groupList = new ArrayList<>();
        deviceList.add("d_" + 0);

        for (String d : deviceList) {
            groupList.add(getGroup(d));
        }
        List<String> uniqueGroupList = new ArrayList<>(new TreeSet<>(groupList));
        tags.put("group", uniqueGroupList);
        tags.put("device", deviceList);
        subQuery.put("tags", tags);

        List<Map<String, Object>> aggList = new ArrayList<>();
        Map<String, Object> aggMap = new HashMap<>();
        aggMap.put(NAME, "last");
        Map<String, Object> samplingMap = new HashMap<>();

        samplingMap.put("value", 1000);

        samplingMap.put("unit", "years");
        aggMap.put("sampling", samplingMap);
        aggList.add(aggMap);

        subQuery.put(AGGREGATORS, aggList);


        List<Map<String, Object>> groupByList = new ArrayList<>();

        Map<String, Object> groupByTagsMap = new HashMap<>();
        groupByTagsMap.put(NAME, "tag");
        List<String> groupByTagsList = new ArrayList<>();
        groupByTagsList.add("device");
        groupByTagsMap.put("tags", groupByTagsList);
        groupByList.add(groupByTagsMap);

        subQuery.put("group_by", groupByList);

        list.add(subQuery);
        queryMap.put(METRICS, list);
        String sql = JSON.toJSONString(queryMap);
        LOGGER.debug("JSON in getTotalTimeInterval(): " + sql);
        String str = null;
        try {
            str = HttpRequest.sendPost(queryUrl, sql);
        } catch (IOException e) {
            LOGGER.error("Failed to get the last time value.");
            e.printStackTrace();
        }

        long lastTimestamp = 0;

        JSONArray jsonArrayQueries = JSON.parseObject(str).getJSONArray("queries");
        for (int i = 0; i < jsonArrayQueries.size(); i++) {
            JSONObject json = jsonArrayQueries.getJSONObject(i);
            JSONArray results = json.getJSONArray("results");
            for (int j = 0; j < results.size(); j++) {
                JSONObject resultJSON = results.getJSONObject(j);
                JSONArray valueArray = resultJSON.getJSONArray("values");
                lastTimestamp = valueArray.getJSONArray(0).getLong(0);
            }
        }

        return lastTimestamp - Constants.START_TIMESTAMP;
    }


    /*
    query JSON example
{
   "start_absolute": 1357023600000,
   "end_relative": {
       "value": "5",
       "unit": "days"
   },
   "time_zone": "Asia/Kabul",
   "metrics": [
       {
           "tags": {
               "HOST": ["foo", "foo2"],
               "customer": ["bar"]
           },
           "name": "abc.123",
           "limit": 10000,
           "aggregators": [
               {
                   "name": "sum",
                   "sampling": {
                       "value": 10,
                       "unit": "minutes"
                   }
               }
           ]
       },
       {
           "tags": {
               "HOST": ["foo", "foo2"],
               "customer": ["bar"]
           },
           "name": "xyz.123",
           "aggregators": [
               {
                   "name": "avg",
                   "sampling": {
                       "value": 10,
                       "unit": "minutes"
                   }
               }
           ]
       }
   ]
}
     */

    /**
     * @param devices     Devices index for this query client
     * @param isAggregate Mark whether add time aggregator in JSON or not
     * @param isLimit     Mark whether add limit in query
     * @param isGroupBy   Mark whether add group by in JSON, group by device is in all query by default
     * @return
     */
    private List<Map<String, Object>> getSubQueries(List<Integer> devices, boolean isAggregate, boolean isLimit, boolean isGroupBy) {
        List<Map<String, Object>> list = new ArrayList<>();

        List<String> sensorList = new ArrayList<>(config.SENSOR_CODES);
        Collections.shuffle(sensorList, sensorRandom);

        for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
            String metric = sensorList.get(i);
            Map<String, Object> subQuery = new HashMap<String, Object>();
            subQuery.put(NAME, metric);
            Map<String, List<String>> tags = new HashMap<>();
            List<String> deviceList = new ArrayList<>();
            List<String> groupList = new ArrayList<>();
            for (int d : devices) {
                deviceList.add("d_" + d);
            }
            for (String d : deviceList) {
                groupList.add(getGroup(d));
            }
            List<String> uniqueGroupList = new ArrayList<>(new TreeSet<>(groupList));
            tags.put("group", uniqueGroupList);
            tags.put("device", deviceList);
            subQuery.put("tags", tags);
            if (isAggregate && !config.QUERY_AGGREGATE_FUN.equals("")) {
                List<Map<String, Object>> aggList = getAggList(isGroupBy);
                subQuery.put(AGGREGATORS, aggList);
            }
            if (isLimit && config.QUERY_LIMIT_N >= 0) {
                subQuery.put("limit", config.QUERY_LIMIT_N);
            }
            List<Map<String, Object>> groupByList = new ArrayList<>();

            Map<String, Object> groupByTagsMap = new HashMap<>();
            groupByTagsMap.put(NAME, "tag");
            List<String> groupByTagsList = new ArrayList<>();
            groupByTagsList.add("device");
            groupByTagsMap.put("tags", groupByTagsList);
            groupByList.add(groupByTagsMap);

            if (isGroupBy) {
                Map<String, Object> groupByTimeMap = new HashMap<>();
                groupByTimeMap.put(NAME, "time");
                groupByTimeMap.put("group_count", String.valueOf(config.QUERY_INTERVAL / config.TIME_UNIT));
                Map<String, String> rangeSizeMap = new HashMap<>();
                rangeSizeMap.put("value", String.valueOf(config.TIME_UNIT));
                rangeSizeMap.put("unit", "milliseconds");
                groupByTimeMap.put("range_size", rangeSizeMap);
                groupByList.add(groupByTimeMap);
            }
            subQuery.put("group_by", groupByList);

            list.add(subQuery);
        }

        return list;
    }

    private List<Map<String, Object>> getAggList(boolean isGroupBy) {
        List<Map<String, Object>> aggList = new ArrayList<>();
        Map<String, Object> aggMap = new HashMap<>();
        aggMap.put(NAME, config.QUERY_AGGREGATE_FUN);
        Map<String, Object> samplingMap = new HashMap<>();
        samplingMap.put("unit", "milliseconds");
        if (isGroupBy) {
            samplingMap.put("value", config.TIME_UNIT);
        } else {
            if (config.QUERY_CHOICE == 10) {
                //samplingMap.put("unit", "years");
                samplingMap.put("value", config.TIME_UNIT);
            } else if(config.QUERY_CHOICE == 6){
                aggMap.put(NAME, "last");
                samplingMap.put("unit", "years");
                samplingMap.put("value", 1000);
            } else if(config.QUERY_CHOICE == 5){
                aggMap.put(NAME, "filter");
                aggMap.put("filter_op", "GT");
                aggMap.put("threshold", config.QUERY_LOWER_LIMIT);
                samplingMap.put("value", config.QUERY_INTERVAL + 1);
            } else {
                // sample by (config.QUERY_INTERVAL + 1) so that the result only contains one point
                samplingMap.put("value", config.QUERY_INTERVAL + 1);
            }
        }
        aggMap.put("sampling", samplingMap);
        aggList.add(aggMap);
        return aggList;
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) {
        String sql = "";
        long startTimeStamp = 0, endTimeStamp = 0, latency = 0;
        Map<String, Object> queryMap = new HashMap<>();
        List<Map<String, Object>> list = null;
        //queryMap.put("time_zone", "Etc/GMT+8");
        //queryMap.put("cache_time", 0);
        queryMap.put(QUERY_START_TIME, startTime);
        queryMap.put(QUERY_END_TIME, startTime + config.QUERY_INTERVAL);

        try {
            List<String> sensorList = new ArrayList<String>();
            switch (config.QUERY_CHOICE) {
                case 1:// 精确点查询
                    long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
                            + Constants.START_TIMESTAMP;
                    if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
                        timeStamp += config.POINT_STEP / 2;
                    }
                    queryMap.put(QUERY_START_TIME, timeStamp);
                    queryMap.put(QUERY_END_TIME, timeStamp);
                    list = getSubQueries(devices, false, false, false);
                    queryMap.put(METRICS, list);
                    break;
                case 2:// 模糊点查询（暂未实现）
                    break;
                case 3:// 聚合函数查询
                    list = getSubQueries(devices, true, false, false);
                    queryMap.put(METRICS, list);
                    break;
                case 4:// 范围查询
                    list = getSubQueries(devices, false, false, false);
                    queryMap.put(METRICS, list);
                    break;
                case 5:// 条件查询: 时间过滤条件 + 值过滤条件
                    list = getSubQueries(devices, false, false, false);
                    queryMap.put(METRICS, list);
                    break;
                case 6:// 最近点查询
                    queryMap.put(QUERY_START_TIME, Constants.START_TIMESTAMP);
                    queryMap.remove(QUERY_END_TIME);
                    list = getSubQueries(devices, true, false, false);
                    queryMap.put(METRICS, list);
                    break;
                case 7:// groupBy查询（暂时只有一个时间段）
                    list = getSubQueries(devices, true, false, true);
                    queryMap.put(METRICS, list);
                    break;
                case 8:// query with limit and series limit and their offsets
                    //not support yet
                    break;
                case 9:// range query with limit
                    list = getSubQueries(devices, false, true, false);
                    queryMap.put(METRICS, list);
                    break;
                case 10:// aggregation function query without any filter
                    list = getSubQueries(devices, true, false, false);
                    queryMap.put(METRICS, list);
                    queryMap.put(QUERY_START_TIME, Constants.START_TIMESTAMP);
                    queryMap.remove(QUERY_END_TIME);
                    break;
                case 11:// aggregation function query with value filter
                    //not support yet
                    break;
            }
            sql = JSON.toJSONString(queryMap);
            LOGGER.info("{} execute {} loop,提交的JSON：{}", Thread.currentThread().getName(), index, sql);
            String str ;
            startTimeStamp = System.nanoTime();
            str = HttpRequest.sendPost(queryUrl, sql);
            endTimeStamp = System.nanoTime();
            latency = endTimeStamp - startTimeStamp;
            latencies.add(latency);
            LOGGER.debug("Response: " + str);

            int pointNum = getOneQueryPointNum(str);
            client.setTotalPoint(client.getTotalPoint() + pointNum);
            client.setTotalTime(client.getTotalTime() + latency);
            LOGGER.info(
                    "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
                            + "TotalTime {}s with totalPoint {} rate is {}points/s",
                    Thread.currentThread().getName(), index, latency / 1000000000.0, pointNum,
                    pointNum * 1000000000.0 / latency, (client.getTotalTime()) / 1000000000.0,
                    client.getTotalPoint(), client.getTotalPoint() * 1000000000.0f / client.getTotalTime());
            mySql.saveQueryProcess(index, pointNum, latency / 1000000000.0f, config.REMARK);
        } catch (Exception e) {
            queryErrorProcess(index, errorCount, sql, startTimeStamp, endTimeStamp, e, LOGGER, mySql);
        }
    }

    private int getOneQueryPointNum(String str) {
        int pointNum = 0;

        JSONArray jsonArrayQueries = JSON.parseObject(str).getJSONArray("queries");
        for (int i = 0; i < jsonArrayQueries.size(); i++) {
            JSONObject json = jsonArrayQueries.getJSONObject(i);
            JSONArray results = json.getJSONArray("results");
            for (int j = 0; j < results.size(); j++) {
                JSONObject resultJSON = results.getJSONObject(j);
                pointNum += resultJSON.getJSONArray("values").size();
            }
        }

        return pointNum;
    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

    }

    @Override
    public long count(String group, String device, String sensor) {
        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {

    }

    @Override
    public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Long> latencies) throws SQLException {

    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException, IOException {

    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random, ArrayList<Long> latencies) throws SQLException {
        return 0;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random, ArrayList<Long> latencies) throws SQLException {
        int timestampIndex;
        PossionDistribution possionDistribution = new PossionDistribution(random);
        int nextDelta;
        LinkedList<String> keys = new LinkedList<>();
        for (int i = 0; i < config.BATCH_SIZE; i++) {
            if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, random)) {
                nextDelta = possionDistribution.getNextPossionDelta();
                timestampIndex = maxTimestampIndex - nextDelta;
            } else {
                maxTimestampIndex++;
                timestampIndex = maxTimestampIndex;
            }
            String key = UUID.randomUUID().toString();
            dataMap.put(key, createDataModel(timestampIndex, device));
            keys.add(key);
        }
        insertOneBatch(keys, loopIndex, totalTime, errorCount, latencies);
        return maxTimestampIndex;
    }

    private LinkedList<KairosDataModel> createDataModel(int timestampIndex, String device) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        String groupId = getGroup(device);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex;
            if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
                currentTime += (long) (config.POINT_STEP * timestampRandom.nextDouble());
            }
            Number value = Function.getValueByFunctionIdAndParam(param, currentTime);
            KairosDataModel model = new KairosDataModel();
            model.setName(sensor);
            // KairosDB do not support float as data type
            model.setType(config.DATA_TYPE.toLowerCase());
            model.setTimestamp(currentTime);
            model.setValue(value);
            Map<String, String> tags = new HashMap<>();
            tags.put("group", groupId);
            tags.put("device", device);
            model.setTags(tags);
            models.addLast(model);
        }
        return models;
    }
}
