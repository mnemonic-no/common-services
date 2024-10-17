package no.mnemonic.services.common.api;

import java.util.HashMap;
import java.util.Map;

/**
 * A ResultSetExtender allows a service to extend the base format of a <code>ResultSet</code>,
 * in particular to add data to the top level object which is not supported
 * by the ServiceProxy transport mechanism.
 *
 * It uses the <code>ServiceProxyMetaDataContext</code> to carry any extra data as metadata,
 * and recreating the <code>ResultSet</code> subclass on the client side.
 *
 * The extender implementation is responsible for extracting metadata (which is not carried by the ResultSet base class)
 * into metadata, and re-create the ResultSet subclass using the same metadata on the client side.
 *
 * <b>Note</b>: The meaning of keys and value of the value format of the metadata map is convention based,
 * and not something that the framework cares about. The server/client implementation should align with
 * which keys are used for what, and what is the expected data format.
 * For this implementation, the server and client implementations are in the same class, so that should be doable.
 *
 */
public interface ResultSetExtender<T extends ResultSet> {

  /**
   * Implement how to extend a generic ResultSet to a subtype, optionally using service proxy metaData
   * @param resultSet the vanilla ResultSet received from the service proxy
   * @param metaData any metadata sent with the ResultSet from the service proxy
   * @return the extended ResultSet object
   */
  T extend(ResultSet resultSet, Map<String, String> metaData);

  /**
   * Extract service proxy metadata from an extended ResultSet object (on the server side)
   *
   * @param extendedResultSet the extended ResultSet object
   * @return the metadata to pass to the service client
   */
  default Map<String, String> extract(T extendedResultSet) {
    return new HashMap<>();
  }

}
