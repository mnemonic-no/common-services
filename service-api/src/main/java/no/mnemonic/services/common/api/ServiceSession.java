package no.mnemonic.services.common.api;

/**
 * The service session should be used in a way that
 * ensures closing of the session when the client exists.
 *
 * Recommended usage is to use try-with-resources:
 * <code>
 *   try (ServiceSession sess = service.openSession()) {
 *     //do service calls
 *   }
 * </code>
 */
public interface ServiceSession extends AutoCloseable {

}
