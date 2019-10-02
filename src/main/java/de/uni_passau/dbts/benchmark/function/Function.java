package de.uni_passau.dbts.benchmark.function;

import java.util.Random;

/** Sensor values generator. */
public class Function {

  /** Random numbers generator. */
  private Random random;

  /**
   * Creates a function instance.
   *
   * @param seed Value to seed the random numbers generator with.
   */
  public Function(int seed) {
    this.random = new Random(seed);
  }

  /**
   * Returns a random value.
   *
   * @param timestamp Timestamp to create a value for.
   * @return A random value.
   */
  public Number get(long timestamp) {
    return 1000 * Math.sin(random.nextDouble() * timestamp) + 1000;
  }
}
