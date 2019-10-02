package de.uni_passau.dbts.benchmark.enums;

/**
 * Aggregation functions.
 */
public enum Aggregation {
  COUNT,
  AVG,
  SUM,
  MAX,
  MIN,
  LAST,
  FIRST,
  TIME_BUCKET,
  MEAN;

  /**
   * Returns an SQL representation of the function applied to the specified column name.
   *
   * @param column Column name.
   * @return SQL aggregation clause.
   */
  public String build(String column) {
    switch (this) {
      case AVG:
      case MAX:
      case MIN:
      case LAST:
      case FIRST:
      case SUM:
      case MEAN:
      case COUNT:
        return this.name() + "(" + column + ")";
      case TIME_BUCKET:
        throw new IllegalArgumentException("Please use the overloaded method for time buckets.");
      default:
        throw new IllegalStateException("Aggregation operation not supported.");
    }
  }

  /**
   * Returns an SQL representation of the time bucket aggregation function.
   *
   * @param column Time column.
   * @param timeBucket Time interval.
   * @return SQL clause.
   */
  public String build(String column, long timeBucket) {
    if (this == TIME_BUCKET) {
      return this.name().toLowerCase()
          + "(interval '"
          + timeBucket
          + " ms', "
          + column
          + ") as time_bucket";
    } else {
      return build(column);
    }
  }
}
