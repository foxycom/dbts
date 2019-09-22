package de.uni_passau.dbts.benchmark.tsdb;

import de.uni_passau.dbts.benchmark.tsdb.citus.Citus;
import de.uni_passau.dbts.benchmark.tsdb.timescaledb.TimescaleDB;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Supported databases by the tool.
 */
public enum DB {
  INFLUXDB,
  CITUS,
  MEMSQL,
  CRATEDB,
  WARP10,
  GRIDDB,
  CLICKHOUSE,
  VERTICA,
  TIMESCALEDB_NARROW,
  TIMESCALEDB_WIDE;

  private Database db;

  /**
   * Calculates the data size of a database.
   *
   * @return Data size in GB.
   * @throws TsdbException if size of data can not be retrieved.
   */
  public float getSize() throws TsdbException {
    float size = 0.0f;
    switch (this) {
      case MEMSQL:
      case CRATEDB:
      case VERTICA:
      case GRIDDB:
      case INFLUXDB:
      case CLICKHOUSE:
      case WARP10:
        break;
      case TIMESCALEDB_NARROW:
        if (db == null) {
          db = new TimescaleDB(TimescaleDB.TableMode.NARROW_TABLE);
          db.init();
        }
        size = db.getSize();
        break;
      case TIMESCALEDB_WIDE:
        if (db == null) {
          db = new TimescaleDB(TimescaleDB.TableMode.WIDE_TABLE);
          db.init();
        }
        size = db.getSize();
        break;
      case CITUS:
        if (db == null) {
          db = new Citus();
          db.init();
        }
        size = db.getSize();
        break;
      default:
        throw new NotImplementedException("");
    }
    return size;
  }
}
