package de.uni_passau.dbts.benchmark.tsdb;

public class TsdbException extends Exception {
  private static final long serialVersionUID = 1L;

  public TsdbException(String message) {
    super(message);
  }

  public TsdbException() {
    super();
  }

  public TsdbException(String message, Throwable cause) {
    super(message, cause);
  }

  public TsdbException(Throwable cause) {
    super(cause);
  }
}
