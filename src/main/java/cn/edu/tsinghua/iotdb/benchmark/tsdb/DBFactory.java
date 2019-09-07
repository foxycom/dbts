package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb.InfluxDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.kairosdb.KairosDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigParser.INSTANCE.config();

  public IDatabase getDatabase() throws SQLException {

    switch (config.DB_SWITCH) {
      case INFLUXDB:
        return new InfluxDB();
      case KAIROSDB:
        return new KairosDB();
      case TIMESCALEDB_NARROW:
        return new TimescaleDB(TimescaleDB.TableMode.NARROW_TABLE);
      case TIMESCALEDB_WIDE:
        return new TimescaleDB(TimescaleDB.TableMode.WIDE_TABLE);
      default:
        LOGGER.error("unsupported database {}", config.DB_SWITCH);
        throw new SQLException("unsupported database " + config.DB_SWITCH);
    }
  }

}
