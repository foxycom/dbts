package de.uni_passau.dbts.benchmark.conf;

import de.uni_passau.dbts.benchmark.utils.TimeUtils;
import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;

/** Constants container. */
public class Constants {

  /** The time of the very first data point. */
  public static final String START_TIME = "2018-8-30T00:00:00Z";

  /** The timestamp of the very first data point */
  public static final long START_TIMESTAMP = TimeUtils.convertDateStrToTimestamp(START_TIME);

  /** Prefix to print in console. */
  public static final String CONSOLE_PREFIX = "dbts>";

  /** The config file. */
  public static final String BENCHMARK_CONF = "conf";

  /** Name of the PostgreSQL JDBC driver. */
  public static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";

  /** PostgreSQL JDBC URL. */
  public static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";

  /** PostgreSQL user. */
  public static final String POSTGRESQL_USER = "postgres";

  /** PostgreSQL password. */
  public static final String POSTGRESQL_PASSWD = "postgres";

  /** Name of the time bucket function. */
  public static final String TIME_BUCKET_ALIAS = "time_bucket";

  /** MySQL JDBC driver name. */
  public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

  /** MySQL JDBC URL. */
  public static final String MYSQL_URL = "jdbc:mysql://%s:%s/%s";

  /** MySQL user name. */
  public static final String MYSQL_USER = "root";

  /** MySQL password. */
  public static final String MYSQL_PASSWD = "";

  /** Vertica JDBC URL. */
  public static final String VERTICA_URL = "jdbc:vertica://%s:%s/%s";

  /** Vertica user name. */
  public static final String VERTICA_USER = "dbadmin";

  /** Vertica password. */
  public static final String VERTICA_PASSWD = "";

  /** CrateDB JDBC driver name. */
  public static final String CRATE_DRIVER = "io.crate.client.jdbc.CrateDriver";

  /** CrateDB JDBC URL. */
  public static final String CRATE_URL = "crate://%s:%s/";

  /** CrateDB user. */
  public static final String CRATE_USER = "crate";

  /** CrateDB password. */
  public static final String CRATE_PASSWD = "";

  /** ClickHouse JDBC driver name. */
  public static final String CLICKHOUSE_DRIVER = "cc.blynk.clickhouse.ClickHouseDriver";

  /** ClickHouse JDBC URL. */
  public static final String CLICKHOUSE_URL = "jdbc:clickhouse://%s:%s/%s";

  /** ClickHouse user name. */
  public static final String CLICKHOUSE_USER = "default";

  /** ClickHouse password. */
  public static final String CLICKHOUSE_PASSWD = "MyStrongPassword";

  /** Nanoseconds to milliseconds convert factor. */
  public static final double NANO_TO_MILLIS = 1000 * 1000d;

  /** Nanoseconds to seconds convert factor. */
  public static final double NANO_TO_SECONDS = 1000 * 1000 * 1000d;

  /** Milliseconds to seconds convert factor. */
  public static final double MILLIS_TO_SECONDS = 1000d;

  /** Default coordinates to use in benchmarks. */
  public static final GeoPoint SPAWN_POINT = new GeoPoint(13.4319466, 48.5667364);
}
