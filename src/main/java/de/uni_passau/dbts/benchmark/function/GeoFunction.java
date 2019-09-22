package de.uni_passau.dbts.benchmark.function;

import de.uni_passau.dbts.benchmark.workload.schema.GeoPoint;

import java.util.Random;

public class GeoFunction {
  private Random random;

  public GeoFunction(int seed) {
    random = new Random(seed);
  }

  public GeoPoint get(GeoPoint center) {
    double dist = random.nextDouble() * 0.03;
    double angle = random.nextDouble() * 2 * Math.PI;

    double deltaLongitude = Math.cos(angle) * dist;
    double deltaLatitude = Math.sin(angle) * dist;

    double newLongitude = center.getLongitude() + deltaLongitude;
    double newLatitude = center.getLatitude() + deltaLatitude;

    return new GeoPoint(newLongitude, newLatitude);
  }
}
