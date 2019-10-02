package de.uni_passau.dbts.benchmark.utils;

import com.github.javafaker.Faker;

/**
 * Random name generator util.
 */
public enum NameGenerator {
  INSTANCE;

  /** Generator instance. */
  private Faker nameFaker;

  /**
   * Creates a name generator instance.
   */
  NameGenerator() {
    nameFaker = new Faker();
  }

  /**
   * Returns a random name.
   *
   * @return A random name.
   */
  public String getName() {
    return nameFaker.name().firstName();
  }
}
