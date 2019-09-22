package de.uni_passau.dbts.benchmark.tsdb;

import de.uni_passau.dbts.benchmark.conf.Config;
import de.uni_passau.dbts.benchmark.conf.ConfigParser;
import de.uni_passau.dbts.benchmark.tsdb.citus.Citus;
import de.uni_passau.dbts.benchmark.tsdb.clickhouse.Clickhouse;
import de.uni_passau.dbts.benchmark.tsdb.cratedb.CrateDB;
import de.uni_passau.dbts.benchmark.tsdb.griddb.GridDB;
import de.uni_passau.dbts.benchmark.tsdb.influxdb.InfluxDB;
import de.uni_passau.dbts.benchmark.tsdb.memsql.MemSQL;
import de.uni_passau.dbts.benchmark.tsdb.timescaledb.TimescaleDB;
import de.uni_passau.dbts.benchmark.tsdb.warp10.Warp10;
import java.sql.SQLException;

import de.uni_passau.dbts.benchmark.tsdb.vertica.Vertica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class DBFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBFactory.class);
  private static Config config = ConfigParser.INSTANCE.config();

  public Database getDatabase() throws SQLException {

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
