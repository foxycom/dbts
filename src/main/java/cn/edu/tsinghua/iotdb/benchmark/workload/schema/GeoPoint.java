package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

public class GeoPoint {
    private double longitude;
    private double latitude;

    public GeoPoint(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
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

    @Override
    public String toString() {
        return String.format("ST_SetSRID(ST_MakePoint(%s, %s),4326)", longitude, latitude);
    }
}
