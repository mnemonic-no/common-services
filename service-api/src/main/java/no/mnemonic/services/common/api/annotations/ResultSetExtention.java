package no.mnemonic.services.common.api.annotations;

import no.mnemonic.services.common.api.ResultSetExtender;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Any method returning a ResultSet will be streamed back to the Service Client.
 * However, if the client interface expects a subtype of ResultSet, the client
 * side needs a ResultSet extender function to convert a plain resultset returned by the
 * Service Client to the type actually declared by the method.
 *
 * To do this, annotate the extended ResultSet type <code>&lt;T&gt;</code> with
 * <code>@ResultSetExtention(extender=MyResultSetExtender.class)</code>,
 * specifying an extender which can handle the result.
 *
 * The extender must be a type with a zero-argument constructor,
 * and implement the ResultSetExtender interface.
 *
 * The extender should make sure to wrap the iterator of the ResultSet to retain
 * streaming capabilities, i.e. iterate each element as it is requested by the client side.
 * If the extender iterates all elements up-front, that will cause the underlying ResultSet to block until
 * all elements are received, effectively disabling the effect of the streaming resultset.
 *
 * To pass extended data into an extended resultset, extend the ResultSet interface with some additional methods,
 * and use the ResultSetExtender.extract()-method to extract extended properties into metadata entries,
 * and then use the ResultSetExtender.extend()-method to parse metadata back into the extended result.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ResultSetExtention {

  Class<? extends ResultSetExtender<?>> extender();

}
