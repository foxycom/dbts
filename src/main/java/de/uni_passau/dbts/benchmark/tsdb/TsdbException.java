package de.uni_passau.dbts.benchmark.tsdb;

/**
 * A general exception that can occur when when working with TSDBs.
 */
public class TsdbException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new exception.
   *
   * @param message Error message.
   */
  public TsdbException(String message) {
    super(message);
  }

  /**
   * Creates a new exception.
   */
  public TsdbException() {
    super();
  }

  /**
   * Creates a new exception.
   *
   * @param message Error message.
   * @param cause The cause.
   */
  public TsdbException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new exception.
   *
   * @param cause The cause.
   */
  public TsdbException(Throwable cause) {
    super(cause);
  }
}
