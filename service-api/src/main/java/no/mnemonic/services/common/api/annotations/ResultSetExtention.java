package no.mnemonic.services.common.api.annotations;

import no.mnemonic.services.common.api.ResultSet;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Function;

/**
 * Any method returning a ResultSet will be streamed back to the ServiceMessageClient.
 * However, if the client interface expects a subtype of ResultSet, the client
 * side needs a ResultSet extender function to convert a plain resultset returned by the
 * ServiceMessageClient to the type actually declared by the method.
 * <p>
 * To do this, annotate the extended ResultSet type <code>&lt;T&gt;</code> with
 * <code>@ResultSetExtention(extender=MyResultSetExtender.class)</code>,
 * specifying an extender which can handle the result.
 * <p>
 * The extender must be a type with a zero-argument constructor,
 * and implement Function&lt;ResultSet, T&gt;.
 * <p>
 * The extender should make sure to wrap the iterator of the ResultSet to retain the
 * streaming capabilities of the messagebus, i.e. iterate each element as it is requested by the client side.
 * If the extender iterates all elements up-front, that will cause the underlying ResultSet to block until
 * all elements are received, effectively disabling the effect of the streaming resultset.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ResultSetExtention {

  Class<? extends Function<ResultSet, ? extends ResultSet>> extender();

}
