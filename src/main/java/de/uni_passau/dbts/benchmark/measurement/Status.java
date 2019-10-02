package de.uni_passau.dbts.benchmark.measurement;

/**
 * Contains information about the execution of an operation.
 */
public class Status {

  /** Whether an operation was successful. */
  private boolean isOk;

  /** Cost time per operation. */
  private long costTime;

  /** The size of a result dataset. */
  private int queryResultPointNum;

  /** Exception that occurred during execution. */
  private Exception exception;

  /** Message of the exception that occurred during execution. */
  private String errorMessage;

  /**
   * Creates a new instance.
   *
   * @param isOk Whether an operation was executed successfully.
   * @param costTime Cost time of an operation.
   * @param exception An exception that occurred if applicable.
   * @param errorMessage Error message of the exception if applicable.
   */
  public Status(boolean isOk, long costTime, Exception exception, String errorMessage) {
    this.isOk = isOk;
    this.costTime = costTime;
    this.exception = exception;
    this.errorMessage = errorMessage;
  }

  /**
   * Creates a new status of an execution without exceptions.
   *
   * @param isOk Whether an operation was executed successfully.
   * @param costTime Cost time of an operation.
   */
  public Status(boolean isOk, long costTime) {
    this.isOk = isOk;
    this.costTime = costTime;
  }

  /**
   * Creates a new instance.
   *
   * @param isOk Whether an operation was executed successfully.
   * @param costTime Cost time.
   * @param queryResultPointNum Number of processed data points.
   */
  public Status(boolean isOk, long costTime, int queryResultPointNum) {
    this.isOk = isOk;
    this.costTime = costTime;
    this.queryResultPointNum = queryResultPointNum;
  }

  /**
   * Creates a new instance.
   *
   * @param isOk Whether an operation was executed successfully.
   * @param costTime Cost time.
   * @param queryResultPointNum Number of data processed data points.
   * @param exception Exception that occurred during execution.
   * @param errorMessage Error message of the exception.
   */
  public Status(
      boolean isOk,
      long costTime,
      int queryResultPointNum,
      Exception exception,
      String errorMessage) {
    this.isOk = isOk;
    this.costTime = costTime;
    this.exception = exception;
    this.errorMessage = errorMessage;
    this.queryResultPointNum = queryResultPointNum;
  }

  /**
   * Returns the number of processed data points.
   *
   * @return Number of processed data points.
   */
  public int getQueryResultPointNum() {
    return queryResultPointNum;
  }

  /**
   * Returns the cost time of an operation.
   *
   * @return Cost time.
   */
  public long getCostTime() {
    return costTime;
  }

  /**
   * Returns the exception that occurred during execution.
   *
   * @return Exception.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Returns the error message of the exception.
   *
   * @return Error message.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Returns true if the operation was executed successfully, false otherwise.
   *
   * @return true if the operation was executed successfully, false otherwise.
   */
  public boolean isOk() {
    return isOk;
  }
}
