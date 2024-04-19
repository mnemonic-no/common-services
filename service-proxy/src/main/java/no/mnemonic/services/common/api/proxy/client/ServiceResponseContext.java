package no.mnemonic.services.common.api.proxy.client;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import no.mnemonic.services.common.api.Resource;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper of a HTTP request and response, implementing Resource
 */
@AllArgsConstructor
public class ServiceResponseContext implements Resource {

    @NonNull
    private final HttpPost request;
    @NonNull
    private final ClassicHttpResponse response;

    @Override
    public void close() throws IOException {
        response.close();
    }

    @Override
    public void cancel() throws IOException {
        request.cancel();
        response.close();
    }

    /**
     * @return content from HTTP response
     * @throws IOException on error reading response
     */
    public InputStream getContent() throws IOException {
        return response.getEntity().getContent();
    }
}
