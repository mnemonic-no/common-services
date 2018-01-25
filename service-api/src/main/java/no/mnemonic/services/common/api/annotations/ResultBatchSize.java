package no.mnemonic.services.common.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation will override the default batch size used by the ServiceMessageHandler
 * when creating the stream of resultset elements to the ServiceMessageClient.
 *
 * Use this annotation for endpoints where the expected element size is large, to limit the
 * size of the messages transported by the message bus (which would cause high latency/timeout), or the
 * need for fragmentation (if supported by the underlying RequestSink implementation).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface ResultBatchSize {

  /**
   * The specified result batch size.
   */
  int value();
}
