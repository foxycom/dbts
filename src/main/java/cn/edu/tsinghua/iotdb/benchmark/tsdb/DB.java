package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.timescaledb.TimescaleDB;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public enum DB {
    IOTDB,
    INFLUXDB,
    OPENTSDB,
    CTSDB,
    KAIROSDB,
    TIMESCALEDB,
    FAKEDB;

    private IDatabase db;

    public float getSize() throws TsdbException {
        float size = 0.0f;
        switch (this) {
            case TIMESCALEDB:
                if (db == null) {
                    db = new TimescaleDB();
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
