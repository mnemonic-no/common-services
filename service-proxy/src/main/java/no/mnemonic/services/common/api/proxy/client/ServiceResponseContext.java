package no.mnemonic.services.common.api.proxy.client;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import no.mnemonic.commons.utilities.AppendMembers;
import no.mnemonic.commons.utilities.AppendUtils;
import no.mnemonic.services.common.api.Resource;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Wrapper of a HTTP request and response, implementing Resource
 */
@AllArgsConstructor
public class ServiceResponseContext implements Resource, AppendMembers {

    private final UUID id = UUID.randomUUID();
    @NonNull
    private final String service;
    @NonNull
    private final String method;
    @NonNull
    private final HttpPost request;
    @NonNull
    private final ClassicHttpResponse response;
    @NonNull
    private Consumer<UUID> onClose;

    public UUID getId() {
        return id;
    }

    @Override
    public String toString() {
        return AppendUtils.toString(this);
    }

    @Override
    public void appendMembers(StringBuilder buf) {
        AppendUtils.appendField(buf, "service", service);
        AppendUtils.appendField(buf, "method", method);
    }

    @Override
    public void close() throws IOException {
        response.close();
        onClose.accept(id);
    }

    @Override
    public void cancel() throws IOException {
        request.cancel();
        response.close();
        onClose.accept(id);
    }

    /**
     * @return content from HTTP response
     * @throws IOException on error reading response
     */
    public InputStream getContent() throws IOException {
        return response.getEntity().getContent();
    }
}
