package no.mnemonic.services.common.api.proxy;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class TestException extends Exception {
  private final int count;

  public TestException(String message, int count) {
    super(message);
    this.count = count;
  }
}
