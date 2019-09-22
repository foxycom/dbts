package de.uni_passau.dbts.benchmark.conf;

import de.uni_passau.dbts.benchmark.enums.Aggregation;
import de.uni_passau.dbts.benchmark.tsdb.DB;
import de.uni_passau.dbts.benchmark.workload.reader.DataSet;

import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;
import java.util.ArrayList;
import java.util.List;

import de.uni_passau.dbts.benchmark.workload.schema.Sensor;

public class Config {
  public Config() {}

  public String HOST = "127.0.0.1";
  public String PORT = "6667";

  public String READ_TOKEN = "";
  public String WRITE_TOKEN = "";

  /** The number of working devices. */
  public int DEVICES_NUMBER = 2;

  public List<SensorGroup> SENSOR_GROUPS = new ArrayList<>();

  /** Assigns a fixed list of devices to each client. */
  public boolean BIND_CLIENTS_TO_DEVICES = true;

  /** The number of thread workers to create. */
  public int CLIENTS_NUMBER = 2;

  /** The number of sensors each bike is equipped with. */
  public int SENSORS_NUMBER = 5;

  /** Time offset that is used to execute each succeeding query, in milliseconds. */
  public int STEP_SIZE = 1;

  public int RADIUS = 500;
  /** 数据发送缓存条数 */
  public int BATCH_SIZE = 10;
  /** 存储组数量 */
  public int DEVICE_GROUPS_NUMBER = 1;
  /** 数据类型 */
  public String DATA_TYPE = "FLOAT";
  /** 数据编码方式 */
  public String ENCODING = "PLAIN";
  /** 生成数据的小数保留位数 */
  public int NUMBER_OF_DECIMAL_DIGIT = 2;
  /** 数据压缩方式 */
  public String COMPRESSOR = "UNCOMPRESSED";
  /** 是否为多设备批插入模式 */
  public boolean MUL_DEV_BATCH = false;
  /** 数据库初始化等待时间ms */
  public long ERASE_WAIT_TIME = 5000;
  /** 是否为批插入乱序模式 */
  public boolean USE_OVERFLOW = false;
  /** 乱序模式 */
  public int OVERFLOW_MODE = 0;
  /** 批插入乱序比例 */
  public double OVERFLOW_RATIO = 1.0;

  public double LAMBDA = 3;

  public int MAX_K = 10;

  public boolean IS_RANDOM_TIMESTAMP_INTERVAL = false;

  public int START_TIMESTAMP_INDEX = 20;

  public String DRIVE_NAME = "sdb";

  public String IFACE_NAME = "enp0s25";

  public int LIMIT_CLAUSE_MODE = 0;

  public String OPERATION_PROPORTION = "1:0:0:0:0:0:0:0:0:0";

  /** 系统性能检测时间间隔-2秒 */
  public int INTERVAL = 0;
  /** 系统性能检测网卡设备名 */
  public String NET_DEVICE = "e";
  /** 存储系统性能信息的文件路径 */
  public String SERVER_MODE_INFO_FILE = "";

  public int SERVER_MONITOR_PORT = 56565;
  /** 一个样例数据的存储组名称 */
  public String STORAGE_GROUP_NAME;
  /** 一个样例数据的时序名称 */
  public String TIMESERIES_NAME;
  /** 一个时序的数据类型 */
  public String TIMESERIES_TYPE;

  /** 样例数据生成路径及文件名 */
  public String GEN_DATA_FILE_PATH = "/home/tim/sampleData";
  /** 上一次结果的日志路径 */
  public String LAST_RESULT_PATH =
      "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/logs";

  /** 存放SQL语句文件的完整路径 */
  public String SQL_FILE = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/SQLFile";

  /** 文件的名字 */
  public String FILE_PATH;

  /** 数据集的名字 */
  public DataSet DATA_SET;

  /** Sensors schema */
  public List<Sensor> FIELDS;

  /** 是否从文件读取数据 */
  public boolean READ_FROM_FILE = false;
  /** 一次插入到数据库的条数 */
  public int BATCH_OP_NUM = 100;

  public boolean TAG_PATH = true;

  public String LOG_STOP_FLAG_PATH;

  public long LOOP = 10000;

  /** Make random generator deterministic again! */
  public long DATA_SEED = 666L;

  /** Sensors list */
  public List<Sensor> SENSORS = new ArrayList<>();

  /** Delete data after benchmark test */
  public boolean ERASE_DATA = false;

  public boolean MONITOR_SERVER = false;

  public double WRITE_RATIO = 0.2;
  public double SIMPLE_QUERY_RATIO = 0.2;
  public double MAX_QUERY_RATIO = 0.2;
  public double RANDOM_INSERT_RATIO = 0.2;
  public double UPDATE_RATIO = 0.2;

  /** Number of sensors to use in queries. */
  public int QUERY_SENSOR_NUM = 1;

  /** Number of devices to perform query on. */
  public int QUERY_DEVICE_NUM = 1;

  /** Which sensor group to perform queries on. */
  public String QUERY_SENSOR_GROUP = "";

  public int QUERY_CHOICE = 1;
  public Aggregation QUERY_AGGREGATE_FUN;
  public long QUERY_INTERVAL = DEVICES_NUMBER;
  public double QUERY_LOWER_LIMIT = 0;
  public boolean IS_EMPTY_PRECISE_POINT_QUERY = false;
  public long TIME_BUCKET = QUERY_INTERVAL / 2;
  public long QUERY_SEED = 1516580959202L;
  public int QUERY_LIMIT_N = 1;
  public int QUERY_LIMIT_OFFSET = 0;
  public int QUERY_SLIMIT_N = 1;
  public int QUERY_SLIMIT_OFFSET = 0;
  public boolean CREATE_SCHEMA = true;

  /** JDBC URL of the MySQL log instance. */
  public String MYSQL_URL =
      "jdbc:mysql://166.111.141.168:3306/benchmark?"
          + "user=root&password=&useUnicode=true&characterEncoding=UTF8&useSSL=false";

  /** Log results in MySQL. */
  public boolean USE_MYSQL = false;

  public long MYSQL_INIT_TIMESTAMP = System.currentTimeMillis();

  public String REMARK = "";
  public String VERSION = "";

  /** Name of the database to use in a DBMS. */
  public String DB_NAME = "test";

  /** Name of DBMS to benchmark. */
  public DB DB_SWITCH = DB.TIMESCALEDB_WIDE;

  public Mode WORK_MODE = Mode.SYNTHETIC_BENCHMARK;

  public long loopTimeRange() {
    return BATCH_SIZE * minSensorTimeStep();
  }

  private long minSensorTimeStep() {
    // FIXME compute min sensor time step
    assert SENSORS != null;
    long minTimeStep = Long.MAX_VALUE;
    for (Sensor sensor : SENSORS) {
      long timeStep = sensor.getInterval();
      if (timeStep < minTimeStep) {
        minTimeStep = timeStep;
      }
    }
    return minTimeStep;
  }
}
