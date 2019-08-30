package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import cn.edu.tsinghua.iotdb.benchmark.client.QueryRealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.client.RealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.client.SyntheticClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ClientMonitoring;
import cn.edu.tsinghua.iotdb.benchmark.monitor.ServerMonitoring;
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
        //Config config = ConfigParser.INSTANCE.config();
        Config config = ConfigParser.INSTANCE.config();
        switch (config.WORK_MODE) {
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
                break;
            case INSERT_TEST_WITH_USERDEFINED_PATH:
                break;
            case QUERY_TEST_WITH_DEFAULT_PATH:
                break;
            case IMPORT_DATA_FROM_CSV:
                break;
            case EXECUTE_SQL_FROM_FILE:
                break;
            case CLIENT_SYSTEM_INFO:
                //clientSystemInfo(config);
                throw new NotImplementedException();
            default:
                throw new SQLException("unsupported mode " + config.WORK_MODE);
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
            if (config.ERASE_DATA) {
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
        CountDownLatch downLatch = new CountDownLatch(config.CLIENTS_NUMBER);
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENTS_NUMBER);
        long st;
        st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENTS_NUMBER);
        for (int i = 0; i < config.CLIENTS_NUMBER; i++) {
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
            if(config.ERASE_DATA){
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
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENTS_NUMBER);

        List<List<String>> thread_files = new ArrayList<>();
        for (int i = 0; i < config.CLIENTS_NUMBER; i++) {
            thread_files.add(new ArrayList<>());
        }

        for (int i = 0; i < files.size(); i++) {
            String filePath = files.get(i);
            int thread = i % config.CLIENTS_NUMBER;
            thread_files.get(thread).add(filePath);
        }

        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENTS_NUMBER);
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENTS_NUMBER);
        for (int i = 0; i < config.CLIENTS_NUMBER; i++) {
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
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENTS_NUMBER);

        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENTS_NUMBER);
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENTS_NUMBER);
        for (int i = 0; i < config.CLIENTS_NUMBER; i++) {
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
              config.OPERATION_PROPORTION, config.WORK_MODE);
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

    private static void serverMode(Config config) {
        ServerMonitoring monitor = ServerMonitoring.INSTANCE;
        try {
            monitor.listen(config);
        } catch (IOException e) {
            LOGGER.error("Could not start server monitor because: {}", e.getMessage());
        }
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

    private static long getErrorNumIoT(ArrayList<Long> totalInsertErrorNums) {
        return getSumOfList(totalInsertErrorNums);
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
