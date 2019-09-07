package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public enum DB {
    INFLUXDB,
    KAIROSDB,
    TIMESCALEDB_NARROW,
    TIMESCALEDB_WIDE;

    private IDatabase db;

    public float getSize() throws TsdbException {
        float size = 0.0f;
        switch (this) {
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
            default:
                throw new NotImplementedException();
        }
        return size;
    }
}
