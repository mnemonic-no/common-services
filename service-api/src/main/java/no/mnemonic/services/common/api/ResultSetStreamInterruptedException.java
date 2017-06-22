package no.mnemonic.services.common.api;

public class ResultSetStreamInterruptedException extends RuntimeException {

  private static final long serialVersionUID = 1348167840449469021L;

  public ResultSetStreamInterruptedException() {
  }

  public ResultSetStreamInterruptedException(String message) {
    super(message);
  }

  public ResultSetStreamInterruptedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ResultSetStreamInterruptedException(Throwable cause) {
    super(cause);
  }
}
