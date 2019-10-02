package de.uni_passau.dbts.benchmark.workload.schema;

import de.uni_passau.dbts.benchmark.tsdb.DB;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Locale;

/**
 * This class represents a basic geographic location which is determined by a longitude and
 * a latitude.
 */
public class GeoPoint {
  private double longitude;
  private double latitude;

  /**
   * Creates a new instance.
   *
   * @param longitude Longitude.
   * @param latitude Latitude.
   */
  public GeoPoint(double longitude, double latitude) {
    this.longitude = longitude;
    this.latitude = latitude;
  }

  /**
   * Creates a new copy of an other {@link GeoPoint}.
   *
   * @param other Other geo-point.
   */
  public GeoPoint(GeoPoint other) {
    this.longitude = other.longitude;
    this.latitude = other.latitude;
  }

  /**
   * Returns the longitude value.
   *
   * @return Longitude.
   */
  public double getLongitude() {
    return longitude;
  }

  /**
   * Sets a new longitude value.
   *
   * @param longitude New longitude.
   */
  public void setLongitude(double longitude) {
    this.longitude = longitude;
  }

  /**
   * Returns the latitude value.
   *
   * @return Latitude.
   */
  public double getLatitude() {
    return latitude;
  }

  /**
   * Sets a new latitude value.
   *
   * @param latitude New latitude.
   */
  public void setLatitude(double latitude) {
    this.latitude = latitude;
  }

  /**
   * Returns the geo-location in a database-specific format.
   *
   * @param currentDb Currently used database.
   * @return Geo-location.
   */
  public String getValue(DB currentDb) {
    switch (currentDb) {
      case MEMSQL:
      case CRATEDB:
        return String.format("'POINT(%s %s)'", longitude, latitude);
      case VERTICA:
        return String.format("POINT(%s %s)", longitude, latitude);
      case CITUS:
      case TIMESCALEDB_WIDE:
      case TIMESCALEDB_NARROW:
        return String.format("ST_SetSRID(ST_MakePoint(%s, %s),4326)", longitude, latitude);
      case INFLUXDB:
      case CLICKHOUSE:
        return String.format(Locale.US, "%f,%f", longitude, latitude);
      case WARP10:
        return String.format(Locale.US, "%f,%f", latitude, longitude);
      default:
        throw new NotImplementedException("");
    }
  }
}
