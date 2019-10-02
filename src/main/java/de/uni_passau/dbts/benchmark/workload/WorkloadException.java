package de.uni_passau.dbts.benchmark.workload;

/**
 * Exception that can occur while generating a workload.
 */
public class WorkloadException extends Exception {
  private static final long serialVersionUID = 8844396756042772132L;

  /**
   * Creates a new instance.
   *
   * @param message Error message.
   */
  public WorkloadException(String message) {
    super(message);
  }

  /**
   * Creates a new instance.
   */
  public WorkloadException() {
    super();
  }

  /**
   * Creates a new instance.
   *
   * @param message Error message.
   * @param cause The cause.
   */
  public WorkloadException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new instance.
   *
   * @param cause The cause.
   */
  public WorkloadException(Throwable cause) {
    super(cause);
  }
}
