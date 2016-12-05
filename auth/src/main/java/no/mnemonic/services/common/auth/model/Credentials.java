package no.mnemonic.services.common.auth.model;

public interface Credentials {

  SubjectIdentity getUserID();

  SecurityLevel getSecurityLevel();

}
