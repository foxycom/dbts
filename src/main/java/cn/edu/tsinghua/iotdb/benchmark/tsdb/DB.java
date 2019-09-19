package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.citus.Citus;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import org.apache.commons.lang.NotImplementedException;

public enum DB {
    INFLUXDB,
    CITUS,
    MEMSQL,
    CRATEDB,
    KAIROSDB,
    WARP10,
    VERTICA,
    TIMESCALEDB_NARROW,
    TIMESCALEDB_WIDE;

    private IDatabase db;

    public float getSize() throws TsdbException {
        float size = 0.0f;
        switch (this) {
            case MEMSQL:
            case CRATEDB:
            case VERTICA:
            case INFLUXDB:
            case KAIROSDB:
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
                throw new NotImplementedException();
        }
        return size;
    }

}
