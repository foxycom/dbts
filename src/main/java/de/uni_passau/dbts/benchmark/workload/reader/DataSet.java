package de.uni_passau.dbts.benchmark.workload.reader;

public enum DataSet {
  REDD("REDD"),
  TDRIVE("TDRIVE"),
  GEOLIFE("GEOLIFE");

  private String name;

  DataSet(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }
}
