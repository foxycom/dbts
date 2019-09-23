package de.uni_passau.dbts.benchmark.conf;

import de.uni_passau.dbts.benchmark.enums.Aggregation;
import de.uni_passau.dbts.benchmark.tsdb.DB;

import de.uni_passau.dbts.benchmark.utils.Sensors;
import de.uni_passau.dbts.benchmark.workload.schema.SensorGroup;
import java.util.ArrayList;
import java.util.List;

import de.uni_passau.dbts.benchmark.workload.schema.Sensor;

public class Config {

  /**
   * Only {@link ConfigParser} should be able to instantiate an object.
   * TODO convert to a true singleton / move to ConfigParser as inner class.
   */
  Config() {}

  /** Host name of the node running the DB to benchmark. */
  public String HOST = "127.0.0.1";

  /** Port number which the DB to benchmark is running on. */
  public String PORT = "6667";

  /** Warp10 read token. */
  public String READ_TOKEN = "";

  /** Warp10 write token. */
  public String WRITE_TOKEN = "";

  /** The number of working devices. */
  public int DEVICES_NUMBER = 2;

  /** Sensor groups number, which is determined implicitly by parsing sensor configuration. */
  public List<SensorGroup> SENSOR_GROUPS = new ArrayList<>();

  /** Assigns a fixed list of devices to each client. */
  public boolean BIND_CLIENTS_TO_DEVICES = true;

  /** The number of thread workers to spawn. */
  public int CLIENTS_NUMBER = 2;

  /** The number of sensors each bike is equipped with. */
  public int SENSORS_NUMBER = 5;

  /** Time offset that is used to execute each succeeding query, in milliseconds. */
  public int STEP_SIZE = 1;

  /** Used to create a radial area with the radius in meters. */
  public int RADIUS = 500;

  /** Determines the maximal number of readings of sensor having the greatest frequency. */
  public int BATCH_SIZE = 10;

  /** Total number of device groups. */
  public int DEVICE_GROUPS_NUMBER = 1;

  /** Delay to wait in ms after erasing data. */
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

  public String OPERATION_PROPORTION = "1:0:0:0:0:0:0:0:0:0";

  public int INTERVAL = 0;

  public int SERVER_MONITOR_PORT = 56565;

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
  public long TIME_BUCKET = QUERY_INTERVAL / 2;
  public long QUERY_SEED = 1516580959202L;
  public boolean CREATE_SCHEMA = true;

  /** JDBC URL of the MySQL log instance. */
  public String MYSQL_URL =
      "jdbc:mysql://166.111.141.168:3306/benchmark?"
          + "user=root&password=&useUnicode=true&characterEncoding=UTF8&useSSL=false";

  /** Log results in MySQL. */
  public boolean USE_MYSQL = false;

  /** Unique number which is used in MySQL log table names to differentiate them. */
  public long MYSQL_INIT_TIMESTAMP = System.currentTimeMillis();

  /** Some remark to insert into the MySQL log table name. */
  public String REMARK = "";

  /** Name of the database to use in a DBMS. */
  public String DB_NAME = "test";

  /** Name of DBMS to benchmark. */
  public DB DB_SWITCH = DB.TIMESCALEDB_WIDE;

  /** The mode to use upon starting dbts. */
  public Mode WORK_MODE = Mode.SYNTHETIC_BENCHMARK;

  /**
   * Returns the temporal range of values in a single batch
   *
   * @return Time range in ms.
   */
  public long loopTimeRange() {
    return BATCH_SIZE * Sensors.minInterval(SENSORS).getInterval();
  }
}
