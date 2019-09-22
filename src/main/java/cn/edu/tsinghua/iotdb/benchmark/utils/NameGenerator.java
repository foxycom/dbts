package cn.edu.tsinghua.iotdb.benchmark.utils;

import com.github.javafaker.Faker;

public enum NameGenerator {
  INSTANCE;

  private Faker nameFaker;

  NameGenerator() {
    nameFaker = new Faker();
  }

  public String getName() {
    return nameFaker.name().firstName();
  }
}
