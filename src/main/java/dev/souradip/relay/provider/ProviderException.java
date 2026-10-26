package dev.souradip.relay.provider;

public class ProviderException extends Exception {

    public enum Kind {
        /** 429. The provider is overloaded or we are over our quota with it. */
        RATE_LIMITED,
        /** 5xx or connection failure. */
        UNAVAILABLE,
        /** No response, or no progress, within the timeout. */
        TIMEOUT,
        /** 4xx other than 429. The request itself is the problem, so retrying elsewhere will not help. */
        REJECTED,
        /** The stream ended or failed after it had started. */
        BROKEN
    }

    private final Kind kind;
    private final String provider;
    private final long retryAfterMillis;

    public ProviderException(Kind kind, String provider, String message) {
        this(kind, provider, message, 0, null);
    }

    public ProviderException(Kind kind, String provider, String message, long retryAfterMillis, Throwable cause) {
        super(provider + ": " + message, cause);
        this.kind = kind;
        this.provider = provider;
        this.retryAfterMillis = retryAfterMillis;
    }

    public Kind kind() {
        return kind;
    }

    public String provider() {
        return provider;
    }

    public long retryAfterMillis() {
        return retryAfterMillis;
    }

    public boolean retryable() {
        return kind != Kind.REJECTED;
    }

    /** A sign the provider is struggling, as opposed to our request being bad. */
    public boolean countsAgainstProvider() {
        return kind != Kind.REJECTED;
    }
}
