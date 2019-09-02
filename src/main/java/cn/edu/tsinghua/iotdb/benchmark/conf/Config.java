package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.enums.Aggregation;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DB;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.DataSet;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.BasicSensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GpsSensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.SensorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionXml;

import static cn.edu.tsinghua.iotdb.benchmark.conf.Constants.GEO_DATA_TYPE;

public class Config {
	private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
	private String deviceCode;

	public Config() {

	}

	public String HOST ="127.0.0.1";
	public String PORT ="6667";

	/** The number of working devices. */
	public int DEVICES_NUMBER = 2;

	public List<SensorGroup> SENSOR_GROUPS = new ArrayList<>();

	/** 设备和客户端是否绑定 */
	public boolean BIND_CLIENTS_TO_DEVICES = true;
	/** 测试客户端线程数量 */
	public int CLIENTS_NUMBER = 2;
	/** 每个设备的传感器数量 */
	public int SENSORS_NUMBER = 5;

	/** 数据采集步长 */
	public long POINT_STEP = 7000;
	/** 查询时间戳变化增加步长 */
	public int STEP_SIZE = 1;
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
	/**是否为多设备批插入模式*/
	public boolean MUL_DEV_BATCH = false;
	/**数据库初始化等待时间ms*/
	public long ERASE_WAIT_TIME =5000;
	/**是否为批插入乱序模式*/
	public boolean USE_OVERFLOW = false;
	/**乱序模式*/
	public int OVERFLOW_MODE = 0;
	/**批插入乱序比例*/
	public double OVERFLOW_RATIO = 1.0;

	public double LAMBDA = 3;

	public int MAX_K = 10;

	public boolean IS_RANDOM_TIMESTAMP_INTERVAL = false ;

	public int START_TIMESTAMP_INDEX = 20;

	public String DRIVE_NAME = "sdb";

	public String IFACE_NAME = "enp0s25";

	public int LIMIT_CLAUSE_MODE = 0;

	public String OPERATION_PROPORTION = "1:0:0:0:0:0:0:0:0:0";

	/**系统性能检测时间间隔-2秒*/
 	public int INTERVAL = 0;
 	/**系统性能检测网卡设备名*/
 	public String NET_DEVICE = "e";
 	/**存储系统性能信息的文件路径*/
 	public String SERVER_MODE_INFO_FILE = "";
 	public int SERVER_MONITOR_PORT = 56565;
	/**一个样例数据的存储组名称*/
 	public String STORAGE_GROUP_NAME;
	/**一个样例数据的时序名称*/
 	public String TIMESERIES_NAME;
	/**一个时序的数据类型*/
 	public String TIMESERIES_TYPE;

	/**样例数据生成路径及文件名*/
	public String GEN_DATA_FILE_PATH = "/home/liurui/sampleData";
	/**上一次结果的日志路径*/
	public String LAST_RESULT_PATH = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/logs";

	/**存放SQL语句文件的完整路径*/
	public String SQL_FILE = "/var/lib/jenkins/workspace/IoTDBWeeklyTest/iotdb-benchmark/SQLFile";

	/** 文件的名字 */
	public String FILE_PATH;

	/** 数据集的名字 */
	public DataSet DATA_SET;

	/** Sensors schema */
	public List<Sensor> FIELDS;

	/** Sensor accuracy of the sensors schema */
	public int[] PRECISION;

	/** 是否从文件读取数据*/
	public boolean READ_FROM_FILE = false;
	/** 一次插入到数据库的条数 */
	public int BATCH_OP_NUM = 100;

	public boolean TAG_PATH = true;

	public String LOG_STOP_FLAG_PATH;

	public int STORE_MODE = 1;

	public long LOOP = 10000;

	/** 数据采集丢失率 */
	public double POINT_LOSE_RATIO = 0.01;
	// ============ Probability proportions ============ // FIXME 传参数时加上这几个参数

	/** Linear function 0.054 */
	public double LINE_RATIO = 0.054;

	/** Fourier series 0.036 */
	// public static double SIN_RATIO=0.386;//0.036
	public double SIN_RATIO = 0.036;// 0.036

	/** Square wave 0.054 */
	public double SQUARE_RATIO = 0.054;

	/** Random numbers 0.512 */
	public double RANDOM_RATIO = 0.512;

	/** Constant numbers 0.352 */
	// public static double CONSTANT_RATIO= 0.002;//0.352
	public double CONSTANT_RATIO = 0.352;// 0.352

	public Map<String, Double> ratio = new HashMap<>();

	/** Make random generator deterministic again! */
	public long DATA_SEED = 666L;

	/** 内置函数参数 */
	public List<FunctionParam> LINE_LIST = new ArrayList<>();
	public List<FunctionParam> SIN_LIST = new ArrayList<>();
	public List<FunctionParam> SQUARE_LIST = new ArrayList<>();
	public List<FunctionParam> RANDOM_LIST = new ArrayList<>();
	public List<FunctionParam> CONSTANT_LIST = new ArrayList<>();
	public List<FunctionParam> GEO_LIST = new ArrayList<>();

	/**
	 * Sensors list
	 */
	public List<Sensor> SENSORS = new ArrayList<>();

	/** Delete data after benchmark test */
	public boolean ERASE_DATA = false;

	public boolean MONITOR_SERVER = false;

	public double WRITE_RATIO = 0.2;
	public double SIMPLE_QUERY_RATIO = 0.2;
	public double MAX_QUERY_RATIO = 0.2;
	public double MIN_QUERY_RATIO = 0.2;
	public double AVG_QUERY_RATIO = 0.2;
	public double COUNT_QUERY_RATIO = 0.2;
	public double SUM_QUERY_RATIO = 0.2;
	public double RANDOM_INSERT_RATIO = 0.2;
	public double UPDATE_RATIO = 0.2;

	//iotDB查询测试相关参数
	public int QUERY_SENSOR_NUM = 1;

	/** Number of devices to perform query on. */
	public int QUERY_DEVICE_NUM = 1;

	/** Which sensor group to perform queries on. */
	public String QUERY_SENSOR_GROUP = "";
	public int QUERY_CHOICE = 1;
	public Aggregation QUERY_AGGREGATE_FUN;
	public long QUERY_INTERVAL = DEVICES_NUMBER * POINT_STEP;
	public double QUERY_LOWER_LIMIT = 0;
	public boolean IS_EMPTY_PRECISE_POINT_QUERY = false;
	public long TIME_BUCKET = QUERY_INTERVAL / 2;
	public long QUERY_SEED = 1516580959202L;
	public int QUERY_LIMIT_N = 1;
	public int QUERY_LIMIT_OFFSET = 0;
	public int QUERY_SLIMIT_N = 1;
	public int QUERY_SLIMIT_OFFSET = 0;
	public boolean CREATE_SCHEMA = true;
	public long REAL_QUERY_START_TIME = 0;
	public long REAL_QUERY_STOP_TIME = Long.MAX_VALUE;

	/** MySQL logging DB HOST and user credentials */
	public String MYSQL_URL = "jdbc:mysql://166.111.141.168:3306/benchmark?"
			+ "user=root&password=Ise_Nel_2017&useUnicode=true&characterEncoding=UTF8&useSSL=false";

	/** Log data in MySQL */
	public boolean USE_MYSQL = false;

	public long MYSQL_INIT_TIMESTAMP = System.currentTimeMillis();

	public String REMARK = "";
	public String VERSION = "";

	/** Benchmarked DB data */
	public String DB_URL = "http://localhost:8086";
	public String DB_NAME = "test";
	
	/** Name of database to benchmark */
	public DB DB_SWITCH = DB.TIMESCALEDB;

	public Mode WORK_MODE = Mode.TEST_WITH_DEFAULT_PATH;

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
