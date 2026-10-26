package dev.souradip.relay.provider;

/**
 * An open streaming response. Not thread safe, except {@link #close()}, which may be called from
 * any thread to cancel the upstream call.
 */
public interface TokenStream extends AutoCloseable {

    /** @return the next piece of text, or null when the model finished normally */
    String next() throws ProviderException;

    /** Usage as reported by the provider, or null if it did not report any. */
    Usage usage();

    /** How many pieces were received so far. */
    int received();

    /** Cancels the upstream call. Safe to call more than once. */
    @Override
    void close();
}
