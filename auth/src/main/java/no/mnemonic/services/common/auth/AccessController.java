package no.mnemonic.services.common.auth;

import no.mnemonic.services.common.auth.model.*;

import java.util.Set;

public interface AccessController<C extends Credentials, S extends SubjectIdentity, O extends OrganizationIdentity, F extends FunctionIdentity> {

  /**
   * Validates that the credentials are valid
   *
   * @param credentials credentials to check
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  void validate(C credentials) throws InvalidCredentialsException;

  /**
   * Check if given credentials give access to specified function, for any organization
   *
   * @param credentials credentials to check
   * @param function    identifier of function to check
   * @return true if the credentials have access to this function (or any parent), for any organization
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  boolean hasPermission(C credentials, F function) throws InvalidCredentialsException;

  /**
   * Check if given credentials give access to specified function, for any organization
   *
   * @param credentials credentials to check
   * @param function    named function to check
   * @return true if the credentials have access to this function (or any parent), for any organization
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  boolean hasPermission(C credentials, NamedFunction function) throws InvalidCredentialsException;

  /**
   * Check if given credentials give access to specified function for specified organization
   *
   * @param credentials  credentials to check
   * @param function     identifier of function to check
   * @param organization identifier of organization to check
   * @return true if the credentials have access to this function (or any parent), for the specified organization (or any parent)
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  boolean hasPermission(C credentials, F function, O organization) throws InvalidCredentialsException;

  /**
   * Check if given credentials give access to specified function for specified organization
   *
   * @param credentials  credentials to check
   * @param function     named function to check
   * @param organization identifier of organization to check
   * @return true if the credentials have access to this function (or any parent), for the specified organization (or any parent)
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  boolean hasPermission(C credentials, NamedFunction function, O organization) throws InvalidCredentialsException;

  /**
   * Return subject identities bound to given credentials
   *
   * @param credentials credentials to check
   * @return all subject identities matching the credentials (the user ID or any parent group ID)
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  Set<S> getSubjectIdentities(C credentials) throws InvalidCredentialsException;

  /**
   * Return organizations available to given credentials
   *
   * @param credentials credentials to check
   * @return a set of identifiers for all organizations available to the credentials
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  Set<O> getAvailableOrganizations(C credentials) throws InvalidCredentialsException;

  /**
   * Return organizations descending from given top level organization
   *
   * @param credentials credentials to use
   * @param topLevelOrg top level organization to check
   * @return a set of identifiers for all organizations which descend from the specified organization (including itself)
   * @throws InvalidCredentialsException if credentials cannot be authenticated
   */
  Set<O> getDescendingOrganizations(C credentials, O topLevelOrg) throws InvalidCredentialsException;

}
