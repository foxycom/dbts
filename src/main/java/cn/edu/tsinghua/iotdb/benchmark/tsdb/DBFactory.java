package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigParser;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.citus.Citus;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.clickhouse.Clickhouse;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.cratedb.CrateDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.griddb.GridDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.influxdb.InfluxDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.memsql.MemSQL;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import java.sql.SQLException;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.vertica.Vertica;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.warp10.Warp10;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigParser.INSTANCE.config();

  public IDatabase getDatabase() throws SQLException {

    switch (config.DB_SWITCH) {
      case INFLUXDB:
        return new InfluxDB();
      case TIMESCALEDB_NARROW:
        return new TimescaleDB(TimescaleDB.TableMode.NARROW_TABLE);
      case TIMESCALEDB_WIDE:
        return new TimescaleDB(TimescaleDB.TableMode.WIDE_TABLE);
      case CITUS:
        return new Citus();
      case MEMSQL:
        return new MemSQL();
      case CRATEDB:
        return new CrateDB();
      case WARP10:
        return new Warp10();
      case VERTICA:
        return new Vertica();
      case CLICKHOUSE:
        return new Clickhouse();
      case GRIDDB:
        return new GridDB();
      default:
        LOGGER.error("unsupported database {}", config.DB_SWITCH);
        throw new SQLException("unsupported database " + config.DB_SWITCH);
    }
  }
}
