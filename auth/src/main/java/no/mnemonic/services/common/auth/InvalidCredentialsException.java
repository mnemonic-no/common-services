package no.mnemonic.services.common.auth;

public class InvalidCredentialsException extends Exception {

  private static final long serialVersionUID = -8627039809281340658L;

  public InvalidCredentialsException(String message) {
    super(message);
  }

}
