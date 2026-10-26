package dev.souradip.relay.provider;

public interface ModelProvider {

    String name();

    /** Blocking call that returns the whole answer. */
    Completion complete(ModelRequest request) throws ProviderException;

    /**
     * Starts a streaming call and returns once the provider has accepted it (status line and
     * headers received), so the caller can still fail over to another provider at this point.
     */
    TokenStream stream(ModelRequest request) throws ProviderException;
}
