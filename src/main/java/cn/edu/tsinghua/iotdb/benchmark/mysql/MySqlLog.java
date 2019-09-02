package cn.edu.tsinghua.iotdb.benchmark.mysql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.conf.Mode;
import cn.edu.tsinghua.iotdb.benchmark.monitor.KPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

public class MySqlLog {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MySqlLog.class);
    private final String SAVE_CONFIG = "insert into CONFIG values(NULL, %s, %s, %s)";
    private final String SAVE_RESULT = "insert into RESULT values(NULL, %s, %s, %s)";
    private Connection mysqlConnection = null;
    private Config config = ConfigParser.INSTANCE.config();
    private String localName = "";
    private long labID;
    private String day = "";
    private String projectID = "";

    public MySqlLog(long labID) {
        this.labID = labID;
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            localName = localhost.getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("Error connecting HOST; UnknownHostException：{}", e.getMessage());
            e.printStackTrace();
        }
        localName = localName.replace("-", "_").replace(".", "_");
    }

    public void initMysql(boolean initTables) {
        projectID = config.WORK_MODE + "_" + config.DB_SWITCH.name() + "_" + config.REMARK + labID;
        if (config.USE_MYSQL) {
            Date date = new Date(labID);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
            day = sdf.format(date);
            try {
                Class.forName(Constants.MYSQL_DRIVENAME);
                mysqlConnection = DriverManager.getConnection(config.MYSQL_URL);
                if (initTables) {
                    initTable();
                }
            } catch (SQLException e) {
                LOGGER.error("mysql 初始化失败，原因是：{}", e.getMessage());
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                LOGGER.error("mysql 连接初始化失败，原因是：{}", e.getMessage());
                e.printStackTrace();
            }
        }

    }

    /**
     * Checks if the benchmark results table exists. If not, creates a new table.
     */
    public void initTable() {
        Statement stat = null;
        try {
            stat = mysqlConnection.createStatement();
            Mode mode = config.WORK_MODE;
            if (/*mode == Mode.SERVER_MODE || */mode == Mode.CLIENT_SYSTEM_INFO) {
                if (!hasTable("SERVER_MODE_" + localName + "_" + day)) {
                    stat.executeUpdate("create table SERVER_MODE_"
                            + localName
                            + "_"
                            + day
                            + "(id BIGINT, "
                            + "cpu_usage DOUBLE,mem_usage DOUBLE,diskIo_usage DOUBLE,net_recv_rate DOUBLE,net_send_rate DOUBLE, pro_mem_size DOUBLE, "
                            + "dataFileSize DOUBLE,infoFizeSize DOUBLE,metadataFileSize DOUBLE,OverflowFileSize DOUBLE, deltaFileSize DOUBLE, walFileSize DOUBLE,"
                            + "tps DOUBLE,MB_read DOUBLE,MB_wrtn DOUBLE,"
                            + "totalFileNum INT, dataFileNum INT, socketNum INT, settledNum INT, infoNum INT,"
                            + "schemaNum INT, metadataNum INT, overflowNum INT, walNum INT, "
                            + "remark varchar(6000), primary key(id))");
                    LOGGER.info("Table SERVER_MODE create success!");
                }
                return;
            }
            switch (config.DB_SWITCH) {
                case IOTDB:
                    if (!hasTable("IOTDB_DATA_MODEL" + "_" + day)) {
                        stat.executeUpdate("create table IOTDB_DATA_MODEL"
                                + "_"
                                + day
                                + " (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), sensor VARCHAR(50) NOT NULL, path VARCHAR(600), type VARCHAR(50), encoding VARCHAR(50))AUTO_INCREMENT = 1;");
                        LOGGER.info("Table IOTDB_DATA_MODEL_{} create success!",
                                day);
                    }
                    break;
                case INFLUXDB:
                    int i = 0,
                            groupId = 0;
                    if (!hasTable("INFLUXDB_DATA_MODEL" + "_" + day)) {
                        stat.executeUpdate("create table INFLUXDB_DATA_MODEL"
                                + "_"
                                + day
                                + " (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), measurement VARCHAR(50), tag VARCHAR(100), field VARCHAR(100), type VARCHAR(50))AUTO_INCREMENT = 1;");
                        LOGGER.info("Table INFLUXDB_DATA_MODEL_ create success!",
                                day);
                    }
                    break;
            }

            if (!hasTable("CONFIG")) {
                stat.executeUpdate("create table CONFIG (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), configuration_item VARCHAR(150), configuration_value VARCHAR(150))AUTO_INCREMENT = 1;");
                LOGGER.info("Table CONFIG create success!");
            }

            if (!hasTable("RESULT")) {
                stat.executeUpdate("create table RESULT (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, projectID VARCHAR(150), result_key VARCHAR(150), result_value VARCHAR(150))AUTO_INCREMENT = 1;");
                LOGGER.info("Table RESULT create success!");
            }

            if (mode == Mode.QUERY_TEST_WITH_DEFAULT_PATH && !hasTable(projectID)) {
                stat.executeUpdate("create table "
                        + projectID
                        + "(id BIGINT, clientName varchar(50), "
                        + "loopIndex INTEGER, point INTEGER, time DOUBLE, cur_rate DOUBLE, remark varchar(6000), primary key(id,clientName))");
                LOGGER.info("Table {} create success!", projectID);
            }

            if (mode != Mode.QUERY_TEST_WITH_DEFAULT_PATH && !hasTable(projectID)) {

                // Creates table for clients statistics
                stat.executeUpdate(getCreateClientsTableSql());
                LOGGER.info("Table {} was successfully created!", projectID + "Clients");

                // Creates single loop table
                stat.executeUpdate(getCreateLoopTableSql());
                LOGGER.info("Table {} was successfully created!", projectID + "Loop");

                // Creates table for a whole measurement
                stat.executeUpdate(getCreateMeasurementTableSql());
                LOGGER.info("Table {} was successfully created!", projectID);

                stat.executeUpdate(getCreateServerMonitorTableSql());
                LOGGER.info("Table {} was successfully created!", projectID + "Server");

            }
        } catch (SQLException e) {
            LOGGER.error("mysql 创建表格失败,原因是：{}", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (stat != null)
                    stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private String getCreateClientsTableSql() {
        String sql = "CREATE TABLE " + projectID + "Clients"
                + "(time BIGINT, clientName varchar(50), operation varchar(100), status varchar(20), "
                + "loopIndex INTEGER, costTime DOUBLE, "
                + "totalTime DOUBLE, rate DOUBLE, okPoint BIGINT, failPoint BIGINT, remark varchar(6000), "
                + "primary key(time, clientName));";
        return sql;
    }

    private String getCreateLoopTableSql() {
        String sql = "CREATE TABLE " + projectID + "Loop"
                + "(time BIGINT, clientName varchar(50), "
                + "loopIndex INTEGER, rate DOUBLE, primary key(time, clientName));";
        return sql;
    }

    private String getCreateMeasurementTableSql() {
        String sql = "CREATE TABLE " + projectID
                + "(time BIGINT, operation varchar(100), costTimeAvg DOUBLE, costTimeP99 DOUBLE, "
                + "costTimeMedian DOUBLE, totalTime DOUBLE, rate DOUBLE, okPoint BIGINT, errorPoint BIGINT, "
                + "remark varchar(6000), primary key(time));";
        return sql;
    }

    private String getCreateServerMonitorTableSql() {
        String sql = "CREATE TABLE " + projectID + "Server "
                + "(time BIGINT, db varchar(50), cpu DOUBLE, mem DOUBLE, swap DOUBLE, ioWrites DOUBLE, ioReads DOUBLE,"
                + " net_recv DOUBLE, net_send DOUBLE, disk_usage DOUBLE, primary key(time));";
        return sql;
    }

    /**
     * Stores the results of an insert benchmark in the database.
     *
     * @param index The loop index.
     * @param costTimeAvg
     * @param totalTime
     * @param errorPoint
     * @param remark
     */
    public void saveInsertProcess(int index, double costTimeAvg,
                                  double totalTime, /*long pointNum, */long errorPoint, String remark) {
        if (config.USE_MYSQL) {
            // TODO different rates for different sensor frequencies
            //double rate = (config.BATCH_SIZE * config.SENSOR_NUMBER / costTime);

            // TODO delete
            int pointNum = 0;


            double rate = pointNum / costTimeAvg;
            if(Double.isInfinite(rate)) {
                rate = 0;
            }
            String mysqlSql = String.format(Locale.US, "insert into " + config.WORK_MODE + "_" + config.DB_SWITCH + "_" + config.REMARK + labID + " values(%d,%s,%d,%f,%f,%f,%d,%s)",
                    System.currentTimeMillis(), "'" + Thread.currentThread().getName() + "'", index,
                    costTimeAvg, totalTime, rate, errorPoint, "'" + remark + "'");
            Statement stat;
            try {
                stat = mysqlConnection.createStatement();
                stat.executeUpdate(mysqlSql);
                stat.close();
            } catch (Exception e) {
                LOGGER.error(
                        "{} save saveInsertProcess info into mysql failed! Error：{}",
                        Thread.currentThread().getName(), e.getMessage());
                LOGGER.error("{}", mysqlSql);
                e.printStackTrace();
            }
        }
    }


    public void saveCompleteMeasurement(String operation, double costTimeAvg, double costTimeP99, double costTimeMedian,
                                       double totalTime, double rate, long okPointNum, long failPointNum, String remark) {
        if (!config.USE_MYSQL) {
            return;
        }
        String logSql = String.format(Locale.US, "INSERT INTO " + projectID + " values (%d, %s, %f, %f, %f, %f, %f, %d, %d, %s)",
                System.currentTimeMillis(), "'" + operation + "'", costTimeAvg, costTimeP99, costTimeMedian,
                totalTime, rate, okPointNum, failPointNum, "'" + remark + "'");
        try (Statement statement = mysqlConnection.createStatement()) {
            statement.executeUpdate(logSql);
        } catch (SQLException e) {
            LOGGER.error("Saving the whole measurement failed. Error: {}", e.getMessage());
            LOGGER.error(logSql);
        }
    }

    public void saveClientInsertProcess(String clientName, String operation, String status, int loopIndex, double costTime,
                                         double totalTime, double rate, long okPointNum, long failPointNum, String remark) {
        if (!config.USE_MYSQL) {
            return;
        }
        String logSql = String.format(Locale.US, "INSERT INTO " + config.WORK_MODE + "_" +
                config.DB_SWITCH + "_" + config.REMARK + labID + "Clients values (%d, %s, %s, %s, %d, %f, %f, %f, %d, %d, %s)",
                System.currentTimeMillis(), "'" + clientName + "'", "'" + operation + "'", "'" + status + "'",
                loopIndex, costTime, totalTime, rate, okPointNum, failPointNum, "'" + remark + "'");
        Statement statement;
        try {
            statement = mysqlConnection.createStatement();
            statement.executeUpdate(logSql);
            statement.close();
        } catch (SQLException e) {
            LOGGER.error("{} saving client insertProcess failed. Error: {}", Thread.currentThread().getName(),
                    e.getMessage());
            LOGGER.error(logSql);
        }
    }

    // 将查询测试的过程数据存入mysql
    public void saveQueryProcess(int index, int point, double time,
                                 String remark) {
        double rate;
        if (config.USE_MYSQL) {
            if (time == 0) {
                remark = "rate is insignificance because time = 0";
                rate = -1;
            } else {
                rate = point / time;
            }
            String mysqlSql = String.format(
                    "insert into " + config.WORK_MODE + "_" + config.DB_SWITCH + "_" + config.REMARK + labID + " values(%d,%s,%d,%d,%f,%f,%s)",
                    System.currentTimeMillis(),
                    "'" + Thread.currentThread().getName() + "'",
                    index, point, time, rate, "'" + remark + "'");
            Statement stat;
            try {
                stat = mysqlConnection.createStatement();
                stat.executeUpdate(mysqlSql);
                stat.close();
            } catch (SQLException e) {
                LOGGER.error(
                        "{} save queryProcess info into mysql failed! Error:{}",
                        Thread.currentThread().getName(), e.getMessage());
                LOGGER.error("{}", mysqlSql);
                e.printStackTrace();
            }
        }
    }

    public void insertServerMetrics(List<KPI> kpis) {
        if (!config.USE_MYSQL) {
            return;
        }
        if (kpis.size() == 0) {
            return;
        }
        String sql = "";

        try (Statement statement = mysqlConnection.createStatement()) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("INSERT INTO ").append(projectID).append("Server VALUES ");

            boolean firstIteration = true;
            for (KPI kpi : kpis) {
                if (firstIteration) {
                    firstIteration = false;
                } else {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append("(").append(kpi.getTimestamp()).append(", '").append(config.DB_SWITCH.name())
                        .append("', ").append(kpi.getCpu()).append(", ").append(kpi.getMem()).append(", ")
                        .append(kpi.getSwap()).append(", ").append(kpi.getIoWrites()).append(", ").append(kpi.getIoReads())
                        .append(", ").append(kpi.getNetRecv()).append(", ").append(kpi.getNetTrans()).append(", ")
                        .append(kpi.getDataSize()).append(")");
            }
            sqlBuilder.append(";");
            sql = sqlBuilder.toString();
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            LOGGER.error("Could not insert server statistics, because: {}", e.getMessage());
            LOGGER.error("{}", sql);
        }
    }

    /* TODO legacy code */
    public void insertSERVER_MODELegacy(double cpu, double mem, double io, double net_recv, double net_send, double pro_mem_size,
                                  double dataSize, double infoSize, double metadataSize, double overflowSize, double deltaSize,double walSize,
                                  float tps, float io_read, float io_wrtn,
                                  List<Integer> openFileList, String remark) {
        if (config.USE_MYSQL) {
            Statement stat = null;
            String sql = "";
            try {
                stat = mysqlConnection.createStatement();
                sql = String.format(Locale.US, "insert into " + projectID + "Server"
                                + " values(%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s)",
                        System.currentTimeMillis(),
                        cpu,
                        mem,
                        io,
                        net_recv,
                        net_send,
                        pro_mem_size,
                        dataSize,
                        infoSize,
                        metadataSize,
                        overflowSize,
                        deltaSize,
                        walSize,
                        tps,
                        io_read,
                        io_wrtn,
                        openFileList.get(0),
                        openFileList.get(1),
                        openFileList.get(2),
                        openFileList.get(3),
                        openFileList.get(4),
                        openFileList.get(5),
                        openFileList.get(6),
                        openFileList.get(7),
                        openFileList.get(8),
                        "'" + remark + "'"
                        );
                stat.executeUpdate(sql);
            } catch (SQLException e) {
                LOGGER.error("Could not insert server statistics, because: {}", e.getMessage());
                LOGGER.error("{}", sql);
                e.printStackTrace();
            } finally {
                if (stat != null) {
                    try {
                        stat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    // 存储IOTDB实验模型
    public void saveIoTDBDataModel(String sensor, String path, String type,
                                   String encoding) {
        if (!config.USE_MYSQL) {
            return;
        }
        Statement stat = null;
        String sql = "";
        try {
            stat = mysqlConnection.createStatement();
            sql = String.format("insert into IOTDB_DATA_MODEL" + "_" + day
                    + " values(NULL, %s, %s, %s, %s, %s)", "'" + projectID
                    + "'", "'" + sensor + "'", "'" + path + "'", "'" + type
                    + "'", "'" + encoding + "'");
            stat.executeUpdate(sql);
        } catch (SQLException e) {
            LOGGER.error("{}将结果信息写入mysql失败，because ：{}", sql, e.getMessage());
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // 存储InfluxDB实验模型
    public void saveInfluxDBDataModel(String measurement, String tag,
                                      String field, String type) {
        if (!config.USE_MYSQL) {
            return;
        }
        Statement stat = null;
        String sql = "";
        try {
            stat = mysqlConnection.createStatement();
            sql = String.format("insert into INFLUXDB_DATA_MODEL" + "_" + day
                    + " values(NULL, %s, %s, %s, %s, %s)", "'" + projectID
                    + "'", "'" + measurement + "'", "'" + tag + "'", "'"
                    + field + "'", "'" + type + "'");
            stat.executeUpdate(sql);
        } catch (SQLException e) {

            LOGGER.error("{}InfluxDBDataModel写入mysql失败，because ：{}", sql, e.getMessage());
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {

                    e.printStackTrace();
                }
            }
        }
    }

    // 存储实验结果
    public void saveResult(String k, String v) {
        if (!config.USE_MYSQL) {
            return;
        }
        Statement stat = null;
        String sql = "";
        try {
            stat = mysqlConnection.createStatement();
            sql = String.format(SAVE_RESULT, "'" + projectID + "'", "'" + k
                    + "'", "'" + v + "'");
            stat.executeUpdate(sql);
        } catch (SQLException e) {

            LOGGER.error("{}将结果信息写入mysql失败，because ：{}", sql, e.getMessage());
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {

                    e.printStackTrace();
                }
            }
        }

    }

    // 存储实验配置信息
    public void saveConfig(String k, String v) {
        if (!config.USE_MYSQL) {
            return;
        }
        Statement stat = null;
        String sql = "";
        try {
            stat = mysqlConnection.createStatement();
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'" + k
                    + "'", "'" + v + "'");
            stat.executeUpdate(sql);
        } catch (SQLException e) {

            LOGGER.error("{}将配置信息写入mysql失败，because ：{}", sql, e.getMessage());
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {

                    e.printStackTrace();
                }
            }
        }

    }

    private String getFullGroupDevicePathByName(String d) {
        String[] spl = d.split("_");
        int id = Integer.parseInt(spl[1]);
        int groupSize = config.DEVICES_NUMBER / config.DEVICE_GROUPS_NUMBER;
        int groupIndex = id / groupSize;
        return Constants.ROOT_SERIES_NAME + ".group_" + groupIndex + ".";
                //+ config.DEVICE_CODES.get(id);
    }

    public void saveTestConfig() {
        if (!config.USE_MYSQL) {
            return;
        }
        Statement stat = null;
        String sql = "";
        try {
            stat = mysqlConnection.createStatement();
            if (config.WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'MODE'", "'GEN_DATA_MODE'");
                stat.addBatch(sql);
            } else if (config.WORK_MODE.equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'MODE'", "'QUERY_TEST_MODE'");
                stat.addBatch(sql);
            } else {
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'MODE'", "'INSERT_TEST_MODE'");
                stat.addBatch(sql);
            }
            switch (config.DB_SWITCH) {
                case IOTDB:
                case TIMESCALEDB:
                    sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                            "'ServerIP'", "'" + config.HOST + "'");
                    stat.addBatch(sql);
                    break;
                case INFLUXDB:
                case OPENTSDB:
                case KAIROSDB:
                case CTSDB:
                    String TSHost = config.DB_URL.substring(config.DB_URL.lastIndexOf('/') + 1, config.DB_URL.lastIndexOf(':'));
                    sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                            "'ServerIP'", "'" + TSHost + "'");
                    stat.addBatch(sql);
                    break;
                default:
                    throw new SQLException("unsupported database " + config.DB_SWITCH);
            }
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                    "'CLIENT'", "'" + localName + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                    "'DB_SWITCH'", "'" + config.DB_SWITCH + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                    "'VERSION'", "'" + config.VERSION + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                    "'CLIENT_NUMBER'", "'" + config.CLIENTS_NUMBER + "'");
            stat.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'LOOP'",
                    "'" + config.LOOP + "'");
            stat.addBatch(sql);
            if (config.WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'STORAGE_GROUP_NAME'", "'" + config.STORAGE_GROUP_NAME + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'TIMESERIES_NAME'", "'" + config.TIMESERIES_NAME + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'TIMESERIES_TYPE'", "'" + config.TIMESERIES_TYPE + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'ENCODING'", "'" + config.ENCODING + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'BATCH_SIZE'", "'" + config.BATCH_SIZE + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'POINT_STEP'", "'" + config.POINT_STEP + "'");
                stat.addBatch(sql);
            } else if (config.WORK_MODE.equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {// 查询测试
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'查询数据集存储组数'",
                        "'" + config.DEVICE_GROUPS_NUMBER + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'查询数据集设备数'", "'" + config.DEVICES_NUMBER
                                + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'查询数据集传感器数'", "'" + config.SENSORS_NUMBER
                                + "'");
                stat.addBatch(sql);
                if (config.DB_SWITCH.equals(Constants.DB_IOT)) {
                    sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                            "'IOTDB编码方式'", "'" + config.ENCODING + "'");
                    stat.addBatch(sql);
                }

                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'QUERY_CHOICE'",
                        "'" + Constants.QUERY_CHOICE_NAME[config.QUERY_CHOICE]
                                + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'QUERY_DEVICE_NUM'", "'" + config.QUERY_DEVICE_NUM
                                + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'QUERY_SENSOR_NUM'", "'" + config.QUERY_SENSOR_NUM
                                + "'");
                stat.addBatch(sql);
                switch (config.QUERY_CHOICE) {
                    case 1:
                        sql = String
                                .format(SAVE_CONFIG, "'" + projectID + "'",
                                        "'IS_RESULTSET_NULL'",
                                        "'" + config.IS_EMPTY_PRECISE_POINT_QUERY
                                                + "'");
                        stat.addBatch(sql);
                    case 3:
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'QUERY_AGGREGATE_FUN'", "'"
                                        + config.QUERY_AGGREGATE_FUN + "'");
                        stat.addBatch(sql);
                        break;
                    case 4:
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'TIME_INTERVAL'", "'" + config.QUERY_INTERVAL
                                        + "'");
                        stat.addBatch(sql);
                        break;
                    case 5:
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'FILTRATION_CONDITION'", "'values > "
                                        + config.QUERY_LOWER_LIMIT + "'");
                        stat.addBatch(sql);
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'TIME_INTERVAL'", "'" + config.QUERY_INTERVAL
                                        + "'");
                        stat.addBatch(sql);
                        break;
                    case 7:
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'FILTRATION_CONDITION'", "'values > "
                                        + config.QUERY_LOWER_LIMIT + "'");
                        stat.addBatch(sql);
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'TIME_INTERVAL'", "'" + config.QUERY_INTERVAL
                                        + "'");
                        stat.addBatch(sql);
                        sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                                "'TIME_UNIT'", "' " + config.TIME_BUCKET + "'");
                        stat.addBatch(sql);
                        break;
                }
            } else {// 写入测试
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'IS_OVERFLOW'", "'" + config.USE_OVERFLOW + "'");
                stat.addBatch(sql);
                if (config.USE_OVERFLOW) {
                    sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                            "'OVERFLOW_RATIO'", "'" + config.OVERFLOW_RATIO + "'");
                    stat.addBatch(sql);
                }
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'MUL_DEV_BATCH'", "'" + config.MUL_DEV_BATCH + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'DEVICE_NUMBER'", "'" + config.DEVICES_NUMBER + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'GROUP_NUMBER'", "'" + config.DEVICE_GROUPS_NUMBER + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'DEVICE_NUMBER'", "'" + config.DEVICES_NUMBER + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'SENSOR_NUMBER'", "'" + config.SENSORS_NUMBER + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'BATCH_SIZE'", "'" + config.BATCH_SIZE + "'");
                stat.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                        "'POINT_STEP'", "'" + config.POINT_STEP + "'");
                stat.addBatch(sql);
                if (config.DB_SWITCH.equals(Constants.DB_IOT)) {
                    sql = String.format(SAVE_CONFIG, "'" + projectID + "'",
                            "'ENCODING'", "'" + config.ENCODING + "'");
                    stat.addBatch(sql);
                }
            }
            stat.executeBatch();
        } catch (SQLException e) {
            LOGGER.error("{}将配置信息写入mysql失败，because ：{}", sql, e.getMessage());
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void closeMysql() {
        if (config.USE_MYSQL) {
            if (mysqlConnection != null) {
                try {
                    mysqlConnection.close();
                } catch (SQLException e) {
                    LOGGER.error("mysql 连接关闭失败,原因是：{}", e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 数据库中是否已经存在名字为table的表
     */
    private Boolean hasTable(String table) throws SQLException {
        String checkTable = "show tables like \"" + table + "\"";
        Statement stmt = mysqlConnection.createStatement();

        ResultSet resultSet = stmt.executeQuery(checkTable);
        if (resultSet.next()) {
            return true;
        } else {
            return false;
        }
    }

    public Connection getMysqlConnection() {
        return mysqlConnection;
    }

    public String getLocalName() {
        return localName;
    }

    public long getLabID() {
        return labID;
    }

    public void setLabID(long labID) {
        this.labID = labID;
    }

}
