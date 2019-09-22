package de.uni_passau.dbts.benchmark.mysql;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.conf.Constants;
import de.uni_passau.dbts.benchmark.conf.Mode;
import de.uni_passau.dbts.benchmark.monitor.KPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.List;
import java.util.Locale;

public class MySqlLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySqlLog.class);
  private static final String SAVE_CONFIG = "INSERT INTO CONFIG VALUES (NULL, %s, %s, %s)";
  private Connection mysqlConnection = null;
  private Config config = ConfigParser.INSTANCE.config();
  private String localName = "";
  private long lab;
  private String projectID = "";

  /**
   * Instantiates the MySQL log wrapper.
   *
   * @param lab Identifier of the measurement.
   */
  public MySqlLog(long lab) {
    this.lab = lab;
    try {
      InetAddress localhost = InetAddress.getLocalHost();
      localName = localhost.getHostName();
    } catch (UnknownHostException e) {
      LOGGER.error("Error connecting HOST; UnknownHostException：{}", e.getMessage());
      e.printStackTrace();
    }
    localName = localName.replace("-", "_").replace(".", "_");
  }

  /**
   * Connects to the MySQL logging instance.
   *
   * @param initTables If true, initializes tables upon connection.
   */
  public void initMysql(boolean initTables) {
    projectID = config.DB_SWITCH.name() + "_" + config.REMARK + lab;
    if (config.USE_MYSQL) {
      try {
        Class.forName(Constants.MYSQL_DRIVENAME);
        mysqlConnection = DriverManager.getConnection(config.MYSQL_URL);
        if (initTables) {
          initTable();
        }
      } catch (SQLException e) {
        LOGGER.error("Could not connect to MySQL because: {}", e.getMessage());
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        LOGGER.error("MySQL driver not found: {}", e.getMessage());
        e.printStackTrace();
      }
    }
  }

  /** Initializes tables to store benchmark logs in. */
  public void initTable() {
    Statement statement = null;
    try {
      statement = mysqlConnection.createStatement();
      Mode mode = config.WORK_MODE;

      if (!hasTable("CONFIG")) {
        statement.executeUpdate(
            "CREATE TABLE CONFIG (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, "
                + "projectID VARCHAR(150), configuration_item VARCHAR(150), "
                + "configuration_value VARCHAR(150)) AUTO_INCREMENT = 1;");
        LOGGER.info("Table CONFIG was successfully created!");
      }

      if (!hasTable("RESULT")) {
        statement.executeUpdate(
            "CREATE TABLE RESULT (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, "
                + "projectID VARCHAR(150), result_key VARCHAR(150), result_value VARCHAR(150)) "
                + "AUTO_INCREMENT = 1;");
        LOGGER.info("Table RESULT was successfully created!");
      }

      if (mode == Mode.SYNTHETIC_BENCHMARK && !hasTable(projectID)) {

        // Creates table for clients statistics.
        statement.executeUpdate(getCreateClientsTableSql());
        LOGGER.info("Table {} was successfully created!", projectID + "Clients");

        // Creates table for a whole measurement
        statement.executeUpdate(getCreateMeasurementTableSql());
        LOGGER.info("Table {} was successfully created!", projectID);

        statement.executeUpdate(getCreateServerMonitorTableSql());
        LOGGER.info("Table {} was successfully created!", projectID + "Server");
      }
    } catch (SQLException e) {
      LOGGER.error("Could not create MySQL log tables because：{}", e.getMessage());
      e.printStackTrace();
    } finally {
      try {
        if (statement != null) statement.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Generates an SQL query to create thread specific logs table.
   *
   * @return SQL query.
   */
  private String getCreateClientsTableSql() {
    String sql =
        "CREATE TABLE "
            + projectID
            + "Clients"
            + "(time BIGINT, clientName VARCHAR(50), operation VARCHAR(100), status VARCHAR(20), "
            + "loopIndex INTEGER, costTime DOUBLE, "
            + "totalTime DOUBLE, rate DOUBLE, okPoint BIGINT, failPoint BIGINT, remark VARCHAR(6000), "
            + "PRIMARY KEY(time, clientName));";
    return sql;
  }

  /**
   * Generates an SQL query to create a table for the overall measurement.
   *
   * @return SQL query.
   */
  private String getCreateMeasurementTableSql() {
    String sql =
        "CREATE TABLE "
            + projectID
            + " (time BIGINT, operation VARCHAR(100), costTimeAvg DOUBLE, costTimeP99 DOUBLE, "
            + "costTimeMedian DOUBLE, totalTime DOUBLE, rate DOUBLE, okPoint BIGINT, errorPoint BIGINT, "
            + "remark VARCHAR(6000), PRIMARY KEY(time));";
    return sql;
  }

  /**
   * Generates an SQL query to create a table storing server KPI logs.
   *
   * @return SQL query.
   */
  private String getCreateServerMonitorTableSql() {
    String sql =
        "CREATE TABLE "
            + projectID
            + "Server "
            + "(time BIGINT, db varchar(50), cpu DOUBLE, mem DOUBLE, swap DOUBLE, ioWrites DOUBLE, ioReads DOUBLE,"
            + " net_recv DOUBLE, net_send DOUBLE, disk_usage DOUBLE, PRIMARY KEY(time));";
    return sql;
  }

  /**
   * Saves overall measurement results.
   *
   * @param operation Name of the executed operation.
   * @param costTimeAvg Average cost time.
   * @param costTimeP99 99th percentile cost time.
   * @param costTimeMedian Median cost time.
   * @param totalTime Total time used to execute all batches.
   * @param rate Number of points / s.
   * @param okPointNum Number of points which were read / inserted trouble-free.
   * @param failPointNum Number of points which could not be read / inserted.
   * @param remark Any textual remark to the measurement.
   */
  public void saveOverallMeasurement(
      String operation,
      double costTimeAvg,
      double costTimeP99,
      double costTimeMedian,
      double totalTime,
      double rate,
      long okPointNum,
      long failPointNum,
      String remark) {
    if (!config.USE_MYSQL) {
      return;
    }

    try {
      if (mysqlConnection.isClosed() || !mysqlConnection.isValid(1)) {
        initMysql(false);
      }
    } catch (SQLException e) {
      LOGGER.error("Could not check status of MySQL connection while saving overall measurement.");
      throw new IllegalStateException();
    }

    String logSql =
        String.format(
            Locale.US,
            "INSERT INTO " + projectID + " VALUES (%d, %s, %f, %f, %f, %f, %f, %d, %d, %s)",
            System.currentTimeMillis(),
            "'" + operation + "'",
            costTimeAvg,
            costTimeP99,
            costTimeMedian,
            totalTime,
            rate,
            okPointNum,
            failPointNum,
            "'" + remark + "'");
    try (Statement statement = mysqlConnection.createStatement()) {
      statement.executeUpdate(logSql);
    } catch (SQLException e) {
      LOGGER.error("Saving the whole measurement failed because: {}", e.getMessage());
    }
  }

  /**
   * Saves results of a single thread operation.
   *
   * @param clientName Name of the client executing an operation.
   * @param operation Name of the operation.
   * @param status Status of the operation.
   * @param loopIndex Index number of the loop.
   * @param costTime Time used to execute operation.
   * @param totalTime Time used to create and insert all batches on the current client.
   * @param rate Number of points / s.
   * @param okPointNum Number of points which were read / inserted trouble-free.
   * @param failPointNum Number of points which could not be read / inserted.
   * @param remark Any textual remark to the measurement.
   */
  public void saveClientMeasurement(
      String clientName,
      String operation,
      String status,
      int loopIndex,
      double costTime,
      double totalTime,
      double rate,
      long okPointNum,
      long failPointNum,
      String remark) {
    if (!config.USE_MYSQL) {
      return;
    }
    String logSql =
        String.format(
            Locale.US,
            "INSERT INTO "
                + projectID
                + "Clients values (%d, %s, %s, %s, %d, %f, %f, %f, %d, %d, %s)",
            System.currentTimeMillis(),
            "'" + clientName + "'",
            "'" + operation + "'",
            "'" + status + "'",
            loopIndex,
            costTime,
            totalTime,
            rate,
            okPointNum,
            failPointNum,
            "'" + remark + "'");
    Statement statement;
    try {
      statement = mysqlConnection.createStatement();
      statement.executeUpdate(logSql);
      statement.close();
    } catch (SQLException e) {
      LOGGER.error(
          "{} saving client insertProcess failed. Error: {}",
          Thread.currentThread().getName(),
          e.getMessage());
      LOGGER.error(logSql);
    }
  }

  /**
   * Saves collected server metrics.
   *
   * @param kpis The list of server metrics.
   */
  public void saveServerMetrics(List<KPI> kpis) {
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
        sqlBuilder
            .append("(")
            .append(kpi.getTimestamp())
            .append(", '")
            .append(config.DB_SWITCH.name())
            .append("', ")
            .append(kpi.getCpu())
            .append(", ")
            .append(kpi.getMem())
            .append(", ")
            .append(kpi.getSwap())
            .append(", ")
            .append(kpi.getIoWrites())
            .append(", ")
            .append(kpi.getIoReads())
            .append(", ")
            .append(kpi.getNetRecv())
            .append(", ")
            .append(kpi.getNetTrans())
            .append(", ")
            .append(kpi.getDataSize())
            .append(")");
      }
      sqlBuilder.append(";");
      sql = sqlBuilder.toString();
      statement.executeUpdate(sql);
    } catch (SQLException e) {
      LOGGER.error("Could not insert server statistics, because: {}", e.getMessage());
    }
  }

  /** Saves the config used to run a benchmark. */
  public void saveConfig() {
    if (!config.USE_MYSQL) {
      return;
    }
    Statement statement = null;
    String sql = "";
    try {
      statement = mysqlConnection.createStatement();
      if (config.WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'MODE'", "'GEN_DATA_MODE'");
        statement.addBatch(sql);
      } else if (config.WORK_MODE.equals(Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH)) {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'MODE'", "'QUERY_TEST_MODE'");
        statement.addBatch(sql);
      } else {
        sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'MODE'", "'INSERT_TEST_MODE'");
        statement.addBatch(sql);
      }
      sql =
          String.format(SAVE_CONFIG, "'" + projectID + "'", "'ServerIP'", "'" + config.HOST + "'");
      statement.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'CLIENT'", "'" + localName + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG, "'" + projectID + "'", "'DB_SWITCH'", "'" + config.DB_SWITCH + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG, "'" + projectID + "'", "'VERSION'", "'" + config.VERSION + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG,
              "'" + projectID + "'",
              "'CLIENT_NUMBER'",
              "'" + config.CLIENTS_NUMBER + "'");
      statement.addBatch(sql);
      sql = String.format(SAVE_CONFIG, "'" + projectID + "'", "'LOOP'", "'" + config.LOOP + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG, "'" + projectID + "'", "'IS_OVERFLOW'", "'" + config.USE_OVERFLOW + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG,
              "'" + projectID + "'",
              "'MUL_DEV_BATCH'",
              "'" + config.MUL_DEV_BATCH + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG,
              "'" + projectID + "'",
              "'DEVICE_NUMBER'",
              "'" + config.DEVICES_NUMBER + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG,
              "'" + projectID + "'",
              "'GROUP_NUMBER'",
              "'" + config.DEVICE_GROUPS_NUMBER + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG,
              "'" + projectID + "'",
              "'DEVICE_NUMBER'",
              "'" + config.DEVICES_NUMBER + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG,
              "'" + projectID + "'",
              "'SENSOR_NUMBER'",
              "'" + config.SENSORS_NUMBER + "'");
      statement.addBatch(sql);
      sql =
          String.format(
              SAVE_CONFIG, "'" + projectID + "'", "'BATCH_SIZE'", "'" + config.BATCH_SIZE + "'");
      statement.addBatch(sql);
      statement.executeBatch();
    } catch (SQLException e) {
      LOGGER.error("Could not insert CONFIG info because: {}", e.getMessage());
      e.printStackTrace();
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Returns true if the specified table already exists in MySQL.
   *
   * @param table The name of the table.
   * @return True if the table exists, false otherwise.
   */
  private Boolean hasTable(String table) throws SQLException {
    String checkTable = "SHOW TABLES LIKE \"" + table + "\"";
    Statement statement = mysqlConnection.createStatement();

    ResultSet resultSet = statement.executeQuery(checkTable);
    if (resultSet.next()) {
      return true;
    } else {
      return false;
    }
  }
}
