package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.GeoPoint;

/**
 * 系统运行常量值
 */
public class Constants {
    public static final String START_TIME = "2018-8-30T00:00:00Z";
    public static final long START_TIMESTAMP = TimeUtils.convertDateStrToTimestamp(START_TIME);
    public static final String URL = "jdbc:iotdb://%s:%s/";
    public static final String USER = "root";
    public static final String PASSWD = "root";
    public static final String CONSOLE_PREFIX = "IotDB-benchmark>";
    public static final String BENCHMARK_CONF = "benchmark-conf";
    public static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";
    public static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";
    public static final String POSTGRESQL_USER = "postgres";
    public static final String POSTGRESQL_PASSWD = "postgres";
    //support DB names of DB_SWITCH
    public static final String DB_IOT = "IoTDB";
    public static final String DB_INFLUX = "InfluxDB";
    public static final String DB_KAIROS = "KairosDB";
    public static final String DB_TIMESCALE = "TimescaleDB";
    //special DB_SWITCH
    public static final String BENCHMARK_IOTDB = "App";

    public static final String TIME_BUCKET_ALIAS = "time_bucket";

    public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://%s:%s/%s";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWD = "";

    public static final String VERTICA_URL = "jdbc:vertica://%s:%s/%s";
    public static final String VERTICA_USER = "dbadmin";
    public static final String VERTICA_PASSWD = "";

    public static final String CRATE_URL = "jdbc:crate://%s:%s/";
    public static final String CRATE_USER = "crate";
    public static final String CRATE_PASSWD = "";

    public static final String CLICKHOUSE_DRIVER = "cc.blynk.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://%s:%s/%s";
    public static final String CLICKHOUSE_USER = "default";
    public static final String CLICKHOUSE_PASSWD = "MyStrongPassword";

    public static final double NANO_TO_MILLIS = 1000 * 1000d;
    public static final double NANO_TO_SECONDS = 1000 * 1000 * 1000d;
    public static final double MILLIS_TO_SECONDS = 1000d;

    public static final String[] QUERY_CHOICE_NAME = {
            " ",
            "Precise",
            "Fuzzy",
            "Aggregation",
            "Range",
            "Criteria",
            "Nearest Point",
            "Group By",
            "SLimit",
            "Limit Criteria",
            "Aggregation Without Filter",
            "Aggregation With Value Filter"
    };

    public static final String MODE_QUERY_TEST_WITH_DEFAULT_PATH = "queryTestWithDefaultPath";
    public static final String MODE_INSERT_TEST_WITH_USERDEFINED_PATH = "insertTestWithUserDefinedPath";

    public static final String GEO_DATA_TYPE = "GEOGRAPHY";
    public static final GeoPoint SPAWN_POINT = new GeoPoint(13.4319466, 48.5667364);
    public static final GeoPoint GRID_START_POINT = new GeoPoint(13.4109466, 48.5567364);

}
