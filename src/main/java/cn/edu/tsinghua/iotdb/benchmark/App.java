package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.client.QueryRealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.client.RealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.client.SyntheticClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.conf.Mode;
import cn.edu.tsinghua.iotdb.benchmark.db.ClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.QueryClientThread;
import cn.edu.tsinghua.iotdb.benchmark.db.ctsdb.CTSDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.fakedb.FakeDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.influxdb.InfluxDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.iotdb.IoTDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.kairosdb.KairosDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.opentsdb.OpenTSDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.timescaledb.TimescaleDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Resolve;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ClientMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ServerMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.tool.ImportDataFromCSV;
import cn.edu.tsinghua.iotdb.benchmark.tool.MetaDateBuilder;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final double unitTransfer = 1000000.0;
    private static ClientMonitoring clientMonitoring;

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        CommandCli cli = new CommandCli();
        if (!cli.init(args)) {
            return;
        }
        Config config = ConfigDescriptor.getInstance().getConfig();
        Mode mode = Mode.valueOf(config.BENCHMARK_WORK_MODE.trim().toUpperCase());
        switch (mode) {
            case TEST_WITH_DEFAULT_PATH:
                testWithDefaultPath(config);
                break;
            case WRITE_WITH_REAL_DATASET:
                testWithRealDataSet(config);
                break;
            case QUERY_WITH_REAL_DATASET:
                queryWithRealDataSet(config);
                break;
            case SERVER_MODE:
                serverMode(config);
                break;
            case INSERT_TEST_WITH_DEFAULT_PATH:
                insertTest(config);
                break;
            case INSERT_TEST_WITH_USERDEFINED_PATH:
                genData(config);
                break;
            case QUERY_TEST_WITH_DEFAULT_PATH:
                queryTest(config);
                break;
            case IMPORT_DATA_FROM_CSV:
                importDataFromCSV(config);
                break;
            case EXECUTE_SQL_FROM_FILE:
                executeSQLFromFile(config);
                break;
            case CLIENT_SYSTEM_INFO:
                //clientSystemInfo(config);
                throw new NotImplementedException();
            default:
                throw new SQLException("unsupported mode " + config.BENCHMARK_WORK_MODE);
        }

    }

    /**
     * 按比例选择workload执行的测试
     */
    private static void testWithDefaultPath(Config config) {

        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mySql.initMysql(true);
        mySql.saveTestConfig();
        clientMonitoring = ClientMonitoring.INSTANCE;
        clientMonitoring.connect();

        Measurement measurement = new Measurement();
        measurement.setRemark("this is my remark");
        DBWrapper dbWrapper = new DBWrapper(measurement);

        // register schema if needed
        try {
            dbWrapper.init();
            if (config.IS_DELETE_DATA) {
                try {
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.DB_SWITCH, e);
                }
            }
            try {
                DataSchema dataSchema = DataSchema.getInstance();
                List<DeviceSchema> schemaList = new ArrayList<>();
                for(List<DeviceSchema> schemas: dataSchema.getClientBindSchema().values()) {
                    schemaList.addAll(schemas);
                }
                dbWrapper.registerSchema(schemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.DB_SWITCH, e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.DB_SWITCH, e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.DB_SWITCH, e);
            }
        }
        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENT_NUMBER);
        long st;
        st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            SyntheticClient client = new SyntheticClient(i, downLatch, barrier);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    /**
     * 测试真实数据集
     * @param config
     */
    private static void testWithRealDataSet(Config config) {
        MySqlLog mysql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mysql.initMysql(true);
        mysql.saveTestConfig();

        // BATCH_SIZE is points number in this mode
        config.BATCH_SIZE = config.BATCH_SIZE / config.FIELDS.size();

        File dirFile = new File(config.FILE_PATH);
        if (!dirFile.exists()) {
            LOGGER.error(config.FILE_PATH + " does not exit");
            return;
        }

        LOGGER.info("use dataset: {}", config.DATA_SET);

        List<String> files = new ArrayList<>();
        getAllFiles(config.FILE_PATH, files);
        LOGGER.info("total files: {}", files.size());

        Collections.sort(files);

        List<DeviceSchema> deviceSchemaList = BasicReader.getDeviceSchemaList(files, config);

        Measurement measurement = new Measurement();
        DBWrapper dbWrapper = new DBWrapper(measurement);
        // register schema if needed
        try {
            LOGGER.info("start to init database {}", config.DB_SWITCH);
            dbWrapper.init();
            if(config.IS_DELETE_DATA){
                try {
                    LOGGER.info("start to clean old data");
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.DB_SWITCH, e);
                }
            }
            try {
                // register device schema
                LOGGER.info("start to register schema");
                dbWrapper.registerSchema(deviceSchemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.DB_SWITCH, e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.DB_SWITCH, e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.DB_SWITCH, e);
            }
        }
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENT_NUMBER);

        List<List<String>> thread_files = new ArrayList<>();
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            thread_files.add(new ArrayList<>());
        }

        for (int i = 0; i < files.size(); i++) {
            String filePath = files.get(i);
            int thread = i % config.CLIENT_NUMBER;
            thread_files.get(thread).add(filePath);
        }

        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            Client client = new RealDatasetClient(i, downLatch, config, thread_files.get(i), barrier);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    private static void finalMeasure(ExecutorService executorService, CountDownLatch downLatch,
            Measurement measurement, List<Measurement> threadsMeasurements, long st, List<Client> clients) {
        executorService.shutdown();

        try {
            // wait for all clients finish test
            downLatch.await();
            clientMonitoring.shutdown();
        } catch (InterruptedException e) {
            LOGGER.error("Exception occurred during waiting for all threads finish.", e);
            Thread.currentThread().interrupt();
        }
        long en = System.nanoTime();
        LOGGER.info("All clients finished.");

        // sum up all the measurements and calculate statistics
        measurement.setElapseTime((en - st) / Constants.NANO_TO_SECONDS);
        for (Client client : clients) {
            threadsMeasurements.add(client.getMeasurement());
        }
        for (Measurement m : threadsMeasurements) {
            measurement.mergeMeasurement(m);
        }
        // must call calculateMetrics() before using the Metrics
        measurement.calculateMetrics();
        // output results
        measurement.showConfigs();
        measurement.showMeasurements();
        measurement.showMetrics();
        measurement.save();
    }

    /**
     * 测试真实数据集
     * @param config
     */
    private static void queryWithRealDataSet(Config config) {
        MySqlLog mysql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mysql.initMysql(true);
        mysql.saveTestConfig();
        LOGGER.info("use dataset: {}", config.DATA_SET);
        //check whether the parameters are legitimate
        if(!checkParamForQueryRealDataSet(config)){
            return;
        }

        Measurement measurement = new Measurement();
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENT_NUMBER);

        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            Client client = new QueryRealDatasetClient(i, downLatch, barrier, config);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    private static boolean checkParamForQueryRealDataSet(Config config) {
        if(config.QUERY_SENSOR_NUM > config.FIELDS.size()){
          LOGGER.error("QUERY_SENSOR_NUM={} can't greater than size of field, {}.",
              config.QUERY_SENSOR_NUM, config.FIELDS);
          return false;
        }
        String[] split = config.OPERATION_PROPORTION.split(":");
        if(split.length!=Operation.values().length){
          LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
          return false;
        }
        if(!split[0].trim().equals("0")){
          LOGGER.error("OPERATION_PROPORTION {} error, {} can't have write operation.",
              config.OPERATION_PROPORTION, config.BENCHMARK_WORK_MODE);
          return false;
        }
        return true;
    }

    private static void getAllFiles(String strPath, List<String> files) {
        File f = new File(strPath);
        if (f.isDirectory()) {
            File[] fs = f.listFiles();
            for (File f1 : fs) {
                String fsPath = f1.getAbsolutePath();
                getAllFiles(fsPath, files);
            }
        } else if (f.isFile()) {
            files.add(f.getAbsolutePath());
        }
    }

    /**
     * 将数据从CSV文件导入IOTDB
     *
     * @throws SQLException
     */
    private static void importDataFromCSV(Config config) throws SQLException {
        MetaDateBuilder builder = new MetaDateBuilder();
        builder.createMataData(config.METADATA_FILE_PATH);
        ImportDataFromCSV importTool = new ImportDataFromCSV();
        importTool.importData(config.IMPORT_DATA_FILE_PATH);
    }


    // TODO legacy
    /*private static void clientSystemInfo(Config config) {
        double abnormalValue = -1;
        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mySql.initMysql(true);
        File dir = new File(config.LOG_STOP_FLAG_PATH);
        if (dir.exists() && dir.isDirectory()) {
            File file = new File(config.LOG_STOP_FLAG_PATH + "/log_stop_flag");
            int interval = config.INTERVAL;
            HashMap<IoUsage.IOStatistics, Float> ioStatistics;
            // 检测所需的时间在目前代码的参数下至少为2秒
            LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
            while (true) {
                ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
                ArrayList<Float> netUsageList = NetUsage.getInstance().get();
                ArrayList<Integer> openFileList = OpenFileNumber.getInstance().get();
                ioStatistics = IoUsage.INSTANCE.getIOStatistics();
                LOGGER.info("CPU使用率,{}", ioUsageList.get(0));
                LOGGER.info("内存使用率,{}", MemUsage.getInstance().get());
                LOGGER.info("内存使用大小GB,{}", MemUsage.getInstance().getProcessMemUsage());
                LOGGER.info("磁盘IO使用率,{},TPS,{},读速率MB/s,{},写速率MB/s,{}",
                        ioUsageList.get(1),
                        ioStatistics.get(IoUsage.IOStatistics.TPS),
                        ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                        ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
                LOGGER.info("网口接收和发送速率,{},{},KB/s", netUsageList.get(0), netUsageList.get(1));
                LOGGER.info("进程号,{},打开文件总数,{},打开benchmark目录下文件数,{},打开socket数,{}", OpenFileNumber.getInstance().getPid(),
                        openFileList.get(0), openFileList.get(1), openFileList.get(2));
                mySql.insertSERVER_MODE(
                        ioUsageList.get(0),
                        MemUsage.getInstance().get(),
                        ioUsageList.get(1),
                        netUsageList.get(0),
                        netUsageList.get(1),
                        MemUsage.getInstance().getProcessMemUsage(),
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        ioStatistics.get(IoUsage.IOStatistics.TPS),
                        ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                        ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
                        openFileList, "");

                try {
                    Thread.sleep(interval * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (file.exists()) {
                    boolean f = file.delete();
                    if (!f) {
                        LOGGER.error("log_stop_flag 文件删除失败");
                    }
                    break;
                }
            }
        } else {
            LOGGER.error("LOG_STOP_FLAG_PATH not exist!");
        }

        mySql.closeMysql();
    }*/

    private static void serverMode(Config config) {
        ServerMonitoring monitor = ServerMonitoring.INSTANCE;
        try {
            monitor.listen(config);
        } catch (IOException e) {
            LOGGER.error("Could not start server monitor because: {}", e.getMessage());
        }
    }

    private static void executeSQLFromFile(Config config) throws SQLException, ClassNotFoundException {
        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mySql.initMysql(true);
        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);
        IDatebase datebase;
        long exeSQLFromFileStartTime;
        long exeSQLFromFileEndTime;
        float exeSQLFromFileTime = 1;
        int SQLCount = 0;
        try {
            datebase = idbFactory.buildDB(mySql.getLabID());
            datebase.init();
            exeSQLFromFileStartTime = System.nanoTime();
            datebase.exeSQLFromFileByOneBatch();
            datebase.close();
            exeSQLFromFileEndTime = System.nanoTime();
            exeSQLFromFileTime = (exeSQLFromFileEndTime - exeSQLFromFileStartTime) / 1000000000.0f;
        } catch (SQLException e) {
            LOGGER.error("Fail to init database becasue {}", e.getMessage());
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*
         * LOGGER.
         * info("Execute SQL from file {} by one batch cost {} seconds. Mean rate {} SQL/s"
         * , config.SQL_FILE, exeSQLFromFileTime, 1.0f * SQLCount / exeSQLFromFileTime
         * );
         */

        // 加入新版的mysql表中
        mySql.closeMysql();
    }

    private static void genData(Config config) throws SQLException, ClassNotFoundException {
        // 一次生成一个timeseries的数据
        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mySql.initMysql(true);
        mySql.saveTestModel(config.TIMESERIES_TYPE, config.ENCODING);
        mySql.saveTestConfig();
        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);

        IDatebase datebase;
        long createSchemaStartTime;
        long createSchemaEndTime;
        float createSchemaTime;
        try {
            datebase = idbFactory.buildDB(mySql.getLabID());
            datebase.init();
            createSchemaStartTime = System.nanoTime();
            datebase.createSchemaOfDataGen();
            datebase.close();
            createSchemaEndTime = System.nanoTime();
            createSchemaTime = (createSchemaEndTime - createSchemaStartTime) / 1000000000.0f;
        } catch (SQLException e) {
            LOGGER.error("Fail to init database becasue {}", e.getMessage());
            return;
        }

        ArrayList<Long> totalInsertErrorNums = new ArrayList<>();
        ArrayList<ArrayList> latenciesOfClients = new ArrayList<>();
        long totalErrorPoint;

        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        ArrayList<Long> totalTimes = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            executorService.submit(new ClientThread(idbFactory.buildDB(mySql.getLabID()), i, downLatch, totalTimes,
                    totalInsertErrorNums, latenciesOfClients));
        }
        executorService.shutdown();
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long totalTime = 0;
        for (long c : totalTimes) {
            if (c > totalTime) {
                totalTime = c;
            }
        }
        ArrayList<Long> allLatencies = new ArrayList<>();
        int totalOps;
        for (ArrayList<Long> oneClientLatencies : latenciesOfClients) {
            allLatencies.addAll(oneClientLatencies);
        }
        totalOps = allLatencies.size();
        double totalLatency = 0;
        for (long latency : allLatencies) {
            totalLatency += latency;
        }
        float avgLatency = (float) (totalLatency / totalOps / unitTransfer);
        allLatencies.sort(new LongComparator());
        int min = (int) (allLatencies.get(0) / unitTransfer);
        int max = (int) (allLatencies.get(totalOps - 1) / unitTransfer);
        int p1 = (int) (allLatencies.get((int) (totalOps * 0.01)) / unitTransfer);
        int p5 = (int) (allLatencies.get((int) (totalOps * 0.05)) / unitTransfer);
        int p50 = (int) (allLatencies.get((int) (totalOps * 0.5)) / unitTransfer);
        int p90 = (int) (allLatencies.get((int) (totalOps * 0.9)) / unitTransfer);
        int p95 = (int) (allLatencies.get((int) (totalOps * 0.95)) / unitTransfer);
        int p99 = (int) (allLatencies.get((int) (totalOps * 0.99)) / unitTransfer);
        int p999 = (int) (allLatencies.get((int) (totalOps * 0.999)) / unitTransfer);
        int p9999 = (int) (allLatencies.get((int) (totalOps * 0.9999)) / unitTransfer);
        double midSum = 0;
        for (int i = (int) (totalOps * 0.05); i < (int) (totalOps * 0.95); i++) {
            midSum += allLatencies.get(i);
        }
        float midAvgLatency = (float) (midSum / (int) (totalOps * 0.9) / unitTransfer);

        long totalPoints = config.LOOP * config.BATCH_SIZE;
        if (config.DB_SWITCH.equals(Constants.DB_IOT) && config.MUL_DEV_BATCH) {
            totalPoints = config.SENSOR_NUMBER * config.CLIENT_NUMBER * config.LOOP * config.BATCH_SIZE;
        }

        totalErrorPoint = getErrorNum(config, totalInsertErrorNums, datebase);
        LOGGER.info(
                "GROUP_NUMBER = ,{}, DEVICE_NUMBER = ,{}, SENSOR_NUMBER = ,{}, BATCH_SIZE = ,{}, POINT_STEP = ,{}, LOOP = ,{}, MUL_DEV_BATCH = ,{}",
                config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER, config.BATCH_SIZE, config.POINT_STEP,
                config.LOOP, config.MUL_DEV_BATCH);

        LOGGER.info("Loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)", totalPoints,
                totalTime / 1000000000.0f, config.CLIENT_NUMBER,
                1000000000.0f * (totalPoints - totalErrorPoint) / (float) totalTime);

        LOGGER.info("Total Operations {}; Latency(ms): Avg {}, MiddleAvg {}, Min {}, Max {}, p1 {}, p5 {}, p50 {}, p90 {}, p95 {}, p99 {}, p99.9 {}, p99.99 {}",
                totalOps, avgLatency, midAvgLatency, min, max, p1, p5, p50, p90, p95, p99, p999, p9999);

        LOGGER.info("Total error num is {}, create schema cost {},s", totalErrorPoint, createSchemaTime);

        // 加入新版的mysql表中
        mySql.saveResult("createSchemaTime(s)", "" + createSchemaTime);
        mySql.saveResult("totalPoints", "" + totalPoints);
        mySql.saveResult("totalTime(s)", "" + totalTime / 1000000000.0f);
        mySql.saveResult("totalErrorPoint", "" + totalErrorPoint);
        mySql.saveResult("avg", "" + avgLatency);
        mySql.saveResult("middleAvg", "" + midAvgLatency);
        mySql.saveResult("min", "" + min);
        mySql.saveResult("max", "" + max);
        mySql.saveResult("p1", "" + p1);
        mySql.saveResult("p5", "" + p5);
        mySql.saveResult("p50", "" + p50);
        mySql.saveResult("p90", "" + p90);
        mySql.saveResult("p95", "" + p95);
        mySql.saveResult("p99", "" + p99);
        mySql.saveResult("p999", "" + p999);
        mySql.saveResult("p9999", "" + p9999);
        mySql.closeMysql();

    }

    static class LongComparator implements Comparator<Long> {
        @Override
        public int compare(Long p1, Long p2) {
            if (p1 < p2) {
                return -1;
            } else if (Objects.equals(p1, p2)) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    /**
     * 数据库插入测试
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static void insertTest(Config config) throws SQLException, ClassNotFoundException {
        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mySql.initMysql(true);
        ArrayList<ArrayList> latenciesOfClients = new ArrayList<>();
        mySql.saveTestModel("Double", config.ENCODING);
        mySql.saveTestConfig();

        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);

        IDatebase datebase;
        long createSchemaStartTime = 0;
        long createSchemaEndTime;
        float createSchemaTime;

        long insertStartTime = System.nanoTime();
        try {
            datebase = idbFactory.buildDB(mySql.getLabID());
            if (config.CREATE_SCHEMA) {
                datebase.init();
                createSchemaStartTime = System.nanoTime();
                datebase.createSchema();
            }
            datebase.close();
            createSchemaEndTime = System.nanoTime();
            createSchemaTime = (createSchemaEndTime - createSchemaStartTime) / 1000000000.0f;
        } catch (SQLException e) {
            LOGGER.error("Fail to init database becasue {}", e.getMessage());
            return;
        }

        ArrayList<Long> totalInsertErrorNums = new ArrayList<>();
        long totalErrorPoint;
        if (config.READ_FROM_FILE) {
            CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
            ArrayList<Long> totalTimes = new ArrayList<>();
            Storage storage = new Storage();
            ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER + 1);
            executorService.submit(new Resolve(config.FILE_PATH, storage));
            for (int i = 0; i < config.CLIENT_NUMBER; i++) {
                executorService.submit(new ClientThread(idbFactory.buildDB(mySql.getLabID()), i, storage, downLatch,
                        totalTimes, totalInsertErrorNums, latenciesOfClients));
            }
            executorService.shutdown();
            // wait for all threads complete
            try {
                downLatch.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int totalItem = storage.getStoragedProductNum();
            long totalTime = 0;
            for (long c : totalTimes) {
                if (c > totalTime) {
                    totalTime = c;
                }
            }

            ArrayList<Long> allLatencies = new ArrayList<>();
            int totalOps;
            for (ArrayList<Long> oneClientLatencies : latenciesOfClients) {
                allLatencies.addAll(oneClientLatencies);
            }
            totalOps = allLatencies.size();
            double totalLatency = 0;
            for (long latency : allLatencies) {
                totalLatency += latency;
            }
            float avgLatency = (float) (totalLatency / totalOps / unitTransfer);
            allLatencies.sort(new LongComparator());
            int min = (int) (allLatencies.get(0) / unitTransfer);
            int max = (int) (allLatencies.get(totalOps - 1) / unitTransfer);
            int p1 = (int) (allLatencies.get((int) (totalOps * 0.01)) / unitTransfer);
            int p5 = (int) (allLatencies.get((int) (totalOps * 0.05)) / unitTransfer);
            int p50 = (int) (allLatencies.get((int) (totalOps * 0.5)) / unitTransfer);
            int p90 = (int) (allLatencies.get((int) (totalOps * 0.9)) / unitTransfer);
            int p95 = (int) (allLatencies.get((int) (totalOps * 0.95)) / unitTransfer);
            int p99 = (int) (allLatencies.get((int) (totalOps * 0.99)) / unitTransfer);
            int p999 = (int) (allLatencies.get((int) (totalOps * 0.999)) / unitTransfer);
            int p9999 = (int) (allLatencies.get((int) (totalOps * 0.9999)) / unitTransfer);
            double midSum = 0;
            for (int i = (int) (totalOps * 0.05); i < (int) (totalOps * 0.95); i++) {
                midSum += allLatencies.get(i);
            }
            float midAvgLatency = (float) (midSum / (int) (totalOps * 0.9) / unitTransfer);

            LOGGER.info("READ_FROM_FILE = true, TAG_PATH = ,{}, STORE_MODE = ,{}, BATCH_OP_NUM = ,{}", config.TAG_PATH,
                    config.STORE_MODE, config.BATCH_OP_NUM);
            LOGGER.info("loaded ,{}, items in ,{},s with ,{}, workers (mean rate ,{}, items/s)", totalItem,
                    totalTime / 1000000000.0f, config.CLIENT_NUMBER, (1000000000.0f * totalItem) / ((float) totalTime));
            LOGGER.info("Total Operations {}; Latency(ms): Avg {}, MiddleAvg {}, Min {}, Max {}, p1 {}, p5 {}, p50 {}, p90 {}, p95 {}, p99 {}, p99.9 {}, p99.99 {}",
                    totalOps, avgLatency, midAvgLatency, min, max, p1, p5, p50, p90, p95, p99, p999, p9999);

        } else {
            CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
            ArrayList<Long> totalTimes = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
            for (int i = 0; i < config.CLIENT_NUMBER; i++) {
                executorService.submit(new ClientThread(idbFactory.buildDB(mySql.getLabID()), i, downLatch, totalTimes,
                        totalInsertErrorNums, latenciesOfClients));
            }
            executorService.shutdown();
            try {
                downLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long totalTime = 0;
            for (long c : totalTimes) {
                if (c > totalTime) {
                    totalTime = c;
                }
            }

            ArrayList<Long> allLatencies = new ArrayList<>();
            int totalOps;
            for (ArrayList<Long> oneClientLatencies : latenciesOfClients) {
                allLatencies.addAll(oneClientLatencies);
            }
            totalOps = allLatencies.size();
            double totalLatency = 0;
            for (long latency : allLatencies) {
                totalLatency += latency;
            }
            float avgLatency = (float) (totalLatency / totalOps / unitTransfer);
            allLatencies.sort(new LongComparator());
            int min = (int) (allLatencies.get(0) / unitTransfer);
            int max = (int) (allLatencies.get(totalOps - 1) / unitTransfer);
            int p1 = (int) (allLatencies.get((int) (totalOps * 0.01)) / unitTransfer);
            int p5 = (int) (allLatencies.get((int) (totalOps * 0.05)) / unitTransfer);
            int p50 = (int) (allLatencies.get((int) (totalOps * 0.5)) / unitTransfer);
            int p90 = (int) (allLatencies.get((int) (totalOps * 0.9)) / unitTransfer);
            int p95 = (int) (allLatencies.get((int) (totalOps * 0.95)) / unitTransfer);
            int p99 = (int) (allLatencies.get((int) (totalOps * 0.99)) / unitTransfer);
            int p999 = (int) (allLatencies.get((int) (totalOps * 0.999)) / unitTransfer);
            int p9999 = (int) (allLatencies.get((int) (totalOps * 0.9999)) / unitTransfer);
            double midSum = 0;
            for (int i = (int) (totalOps * 0.05); i < (int) (totalOps * 0.95); i++) {
                midSum += allLatencies.get(i);
            }
            float midAvgLatency = (float) (midSum / (int) (totalOps * 0.9) / unitTransfer);

            long totalPoints = config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP * config.BATCH_SIZE;
            if (config.DB_SWITCH.equals(Constants.DB_IOT) && config.MUL_DEV_BATCH) {
                totalPoints = config.SENSOR_NUMBER * config.CLIENT_NUMBER * config.LOOP * config.BATCH_SIZE;
            }
            long insertEndTime = System.nanoTime();
            float insertElapseTime = (insertEndTime - insertStartTime) / 1000000000.0f;
            totalErrorPoint = getErrorNum(config, totalInsertErrorNums, datebase);
            LOGGER.info(
                    "Config: \n " +
                            "GROUP_NUMBER = ,{}, \n" +
                            "DEVICE_NUMBER = ,{}, \n" +
                            "SENSOR_NUMBER = ,{}, \n" +
                            "BATCH_SIZE = ,{}, \n" +
                            "POINT_STEP = ,{}, \n" +
                            "LOOP = ,{}, \n" +
                            "MUL_DEV_BATCH = ,{} \n",
                    config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER, config.BATCH_SIZE,
                    config.POINT_STEP, config.LOOP, config.MUL_DEV_BATCH);

            LOGGER.info("Loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)", totalPoints,
                    totalTime / 1000000000.0f, config.CLIENT_NUMBER,
                    1000000000.0f * (totalPoints - totalErrorPoint) / (float) totalTime);
            LOGGER.info("Total Operations {}; Latency(ms): Avg {}, MiddleAvg {}, Min {}, Max {}, p1 {}, p5 {}, p50 {}, p90 {}, p95 {}, p99 {}, p99.9 {}, p99.99 {}",
                    totalOps, avgLatency, midAvgLatency, min, max, p1, p5, p50, p90, p95, p99, p999, p9999);
            LOGGER.info("Total error num is {}, create schema cost {} second. Total elapse time: {} second", totalErrorPoint, createSchemaTime, insertElapseTime);

            mySql.saveResult("createSchemaTime(s)", "" + createSchemaTime);
            mySql.saveResult("totalPoints", "" + totalPoints);
            mySql.saveResult("totalInsertionTime(s)", "" + totalTime / 1000000000.0f);
            mySql.saveResult("totalElapseTime(s)", "" + insertElapseTime);
            mySql.saveResult("totalErrorPoint", "" + totalErrorPoint);
            mySql.saveResult("avg", "" + avgLatency);
            mySql.saveResult("middleAvg", "" + midAvgLatency);
            mySql.saveResult("min", "" + min);
            mySql.saveResult("max", "" + max);
            mySql.saveResult("p1", "" + p1);
            mySql.saveResult("p5", "" + p5);
            mySql.saveResult("p50", "" + p50);
            mySql.saveResult("p90", "" + p90);
            mySql.saveResult("p95", "" + p95);
            mySql.saveResult("p99", "" + p99);
            mySql.saveResult("p999", "" + p999);
            mySql.saveResult("p9999", "" + p9999);
            mySql.closeMysql();

        } // else--

    }

    private static long getErrorNum(Config config, ArrayList<Long> totalInsertErrorNums, IDatebase datebase)
            throws SQLException {
        long totalErrorPoint;
        switch (config.DB_SWITCH) {
            case INFLUXDB:
                totalErrorPoint = getErrorNumInflux(config, datebase);
                break;
            case IOTDB:
            case OPENTSDB:
            case CTSDB:
            case KAIROSDB:
            case TIMESCALEDB:
                totalErrorPoint = getErrorNumIoT(totalInsertErrorNums);
                break;
            default:
                throw new SQLException("unsupported database " + config.DB_SWITCH);
        }
        return totalErrorPoint;
    }

    private static IDBFactory getDBFactory(Config config) throws SQLException {
        switch (config.DB_SWITCH) {
            case IOTDB:
                return new IoTDBFactory();
            case INFLUXDB:
                return new InfluxDBFactory();
            case OPENTSDB:
                return new OpenTSDBFactory();
            case CTSDB:
                return new CTSDBFactory();
            case KAIROSDB:
                return new KairosDBFactory();
            case TIMESCALEDB:
                return new TimescaleDBFactory();
            case FAKEDB:
                return new FakeDBFactory();
            default:
                throw new SQLException("unsupported database " + config.DB_SWITCH);
        }
    }

    private static long getErrorNumInflux(Config config, IDatebase database) {
        // 同一个device中不同sensor的点数是相同的，因此不对sensor遍历
        long insertedPointNum = 0;
        int groupIndex = 0;
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        for (int i = 0; i < config.DEVICE_NUMBER; i++) {
            groupIndex = i / groupSize;
            insertedPointNum += database.count("group_" + groupIndex, "d_" + i, "s_0") * config.SENSOR_NUMBER;
        }
        try {
            database.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP * config.BATCH_SIZE - insertedPointNum;
    }

    private static long getErrorNumIoT(ArrayList<Long> totalInsertErrorNums) {
        return getSumOfList(totalInsertErrorNums);
    }

    /**
     * 数据库查询测试
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static void queryTest(Config config) throws SQLException, ClassNotFoundException {
        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);
        ArrayList<ArrayList> latenciesOfClients = new ArrayList<>();
        MySqlLog mySql = new MySqlLog(config.MYSQL_INIT_TIMESTAMP);
        mySql.initMysql(true);
        mySql.saveTestModel("Double", config.ENCODING);
        mySql.saveTestConfig();

        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        ArrayList<Long> totalTimes = new ArrayList<>();
        ArrayList<Long> totalPoints = new ArrayList<>();
        ArrayList<Long> totalQueryErrorNums = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            executorService.submit(new QueryClientThread(idbFactory.buildDB(mySql.getLabID()), i, downLatch, totalTimes,
                    totalPoints, totalQueryErrorNums, latenciesOfClients));
        }
        executorService.shutdown();
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long totalTime = 0;
        for (long c : totalTimes) {
            if (c > totalTime) {
                totalTime = c;
            }
        }
        long totalResultPoint = getSumOfList(totalPoints);

        ArrayList<Long> allLatencies = new ArrayList<>();
        int totalOps;
        for (ArrayList<Long> oneClientLatencies : latenciesOfClients) {
            allLatencies.addAll(oneClientLatencies);
        }
        totalOps = allLatencies.size();
        double totalLatency = 0;
        for (long latency : allLatencies) {
            totalLatency += latency;
        }
        float avgLatency = (float) (totalLatency / totalOps / unitTransfer);
        allLatencies.sort(new LongComparator());
        int min = (int) (allLatencies.get(0) / unitTransfer);
        int max = (int) (allLatencies.get(totalOps - 1) / unitTransfer);
        int p1 = (int) (allLatencies.get((int) (totalOps * 0.01)) / unitTransfer);
        int p5 = (int) (allLatencies.get((int) (totalOps * 0.05)) / unitTransfer);
        int p50 = (int) (allLatencies.get((int) (totalOps * 0.5)) / unitTransfer);
        int p90 = (int) (allLatencies.get((int) (totalOps * 0.9)) / unitTransfer);
        int p95 = (int) (allLatencies.get((int) (totalOps * 0.95)) / unitTransfer);
        int p99 = (int) (allLatencies.get((int) (totalOps * 0.99)) / unitTransfer);
        int p999 = (int) (allLatencies.get((int) (totalOps * 0.999)) / unitTransfer);
        int p9999 = (int) (allLatencies.get((int) (totalOps * 0.9999)) / unitTransfer);
        double midSum = 0;
        for (int i = (int) (totalOps * 0.05); i < (int) (totalOps * 0.95); i++) {
            midSum += allLatencies.get(i);
        }
        float midAvgLatency = (float) (midSum / (int) (totalOps * 0.9) / unitTransfer);

        LOGGER.info("{}: execute ,{}, query in ,{}, seconds, get ,{}, result points with ,{}, workers (mean rate ,{}, points/s)",
                getQueryName(config), config.CLIENT_NUMBER * config.LOOP, (totalTime / 1000.0f) / 1000000.0, totalResultPoint,
                config.CLIENT_NUMBER, (1000.0f * totalResultPoint) / ((float) totalTime / 1000000.0f));
        LOGGER.info("Total Operations {}; Latency(ms): Avg {}, MiddleAvg {}, Min {}, Max {}, p1 {}, p5 {}, p50 {}, p90 {}, p95 {}, p99 {}, p99.9 {}, p99.99 {}",
                totalOps, avgLatency, midAvgLatency, min, max, p1, p5, p50, p90, p95, p99, p999, p9999);

        long totalErrorPoint = getSumOfList(totalQueryErrorNums);
        LOGGER.info("total error num is {}", totalErrorPoint);

        mySql.saveResult("queryNumber", "" + config.CLIENT_NUMBER * config.LOOP);
        mySql.saveResult("totalPoint", "" + totalResultPoint);
        mySql.saveResult("totalTime(s)", "" + (totalTime / 1000.0f) / 1000000.0);
        mySql.saveResult("resultPointPerSecond(points/s)", "" + (1000.0f * (totalResultPoint)) / (totalTime / 1000000.0));
        mySql.saveResult("totalErrorQuery", "" + totalErrorPoint);
        mySql.saveResult("avgQueryLatency", "" + avgLatency);
        mySql.saveResult("middleAvgQueryLatency", "" + midAvgLatency);
        mySql.saveResult("minQueryLatency", "" + min);
        mySql.saveResult("maxQueryLatency", "" + max);
        mySql.saveResult("p1QueryLatency", "" + p1);
        mySql.saveResult("p5QueryLatency", "" + p5);
        mySql.saveResult("p50QueryLatency", "" + p50);
        mySql.saveResult("p90QueryLatency", "" + p90);
        mySql.saveResult("p95QueryLatency", "" + p95);
        mySql.saveResult("p99QueryLatency", "" + p99);
        mySql.saveResult("p999QueryLatency", "" + p999);
        mySql.saveResult("p9999QueryLatency", "" + p9999);
        mySql.closeMysql();
    }

    private static String getQueryName(Config config) throws SQLException {
        switch (config.QUERY_CHOICE) {
            case 1:
                return "Exact Point Query";
            case 2:
                return "Fuzzy Point Query";
            case 3:
                return "Aggregation Function Query";
            case 4:
                return "Range Query";
            case 5:
                return "Criteria Query";
            case 6:
                return "Nearest Point Query";
            case 7:
                return "Group By Query";
            case 8:
                return "Limit SLimit Query";
            case 9:
                return "Limit Criteria Query";
            case 10:
                return "Aggregation Function Query Without Filter";
            case 11:
                return "Aggregation Function Query With Value Filter";
            default:
                throw new SQLException("unsupported query type " + config.QUERY_CHOICE);
        }
    }

    /**
     * 计算list中所有元素的和
     */
    private static long getSumOfList(ArrayList<Long> list) {
        long total = 0;
        for (long c : list) {
            total += c;
        }
        return total;
    }

}
