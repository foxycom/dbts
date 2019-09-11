package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.tsdb.DB;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Locale;

public class GeoPoint {
    private double longitude;
    private double latitude;

    public GeoPoint(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public GeoPoint(GeoPoint other) {
        this.longitude = other.longitude;
        this.latitude = other.latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getValue(DB currentDb) {
        switch (currentDb) {
            case MEMSQL:
            case CRATEDB:
                return String.format("'POINT(%s %s)'", longitude, latitude);
            case CITUS:
            case TIMESCALEDB_WIDE:
            case TIMESCALEDB_NARROW:
                return String.format("ST_SetSRID(ST_MakePoint(%s, %s),4326)", longitude, latitude);
            case INFLUXDB:
                return String.format(Locale.US, "%f,%f", longitude, latitude);
            default:
                throw new NotImplementedException();
        }
    }
}
