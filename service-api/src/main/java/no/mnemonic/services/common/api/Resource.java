package no.mnemonic.services.common.api;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents a closeable resource. In addition to being Closeable,
 * it can be cancelled!
 */
public interface Resource extends Closeable {

    /**
     * Cancel underlying resource. Cancel may often be the same as close, but in some cases,
     * cancel is more abrupt, forcing resources to close abruptly.
     *
     * @throws IOException if exception occurs when cancelling resources
     */
    default void cancel() throws IOException {
        close();
    }
}
