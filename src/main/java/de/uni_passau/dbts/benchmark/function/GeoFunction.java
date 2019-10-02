package de.uni_passau.dbts.benchmark.function;

import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;

import java.util.Random;

/** Random coordinates generator. */
public class GeoFunction {

  /** Random numbers generator. */
  private Random random;

  /**
   * Creates a geospatial function instance.
   *
   * @param seed Random generator seed.
   */
  public GeoFunction(int seed) {
    random = new Random(seed);
  }

  /**
   * Returns a random location close to the specified ones.
   *
   * @param location Some location.
   * @return Random coordinates.
   */
  public GeoPoint get(GeoPoint location) {
    double dist = random.nextDouble() * 0.03;
    double angle = random.nextDouble() * 2 * Math.PI;

    double deltaLongitude = Math.cos(angle) * dist;
    double deltaLatitude = Math.sin(angle) * dist;

    double newLongitude = location.getLongitude() + deltaLongitude;
    double newLatitude = location.getLatitude() + deltaLatitude;

    return new GeoPoint(newLongitude, newLatitude);
  }
}
