package dev.souradip.relay.routing;

import dev.souradip.relay.provider.ProviderException;
import java.util.List;

/** Every provider was skipped or failed. */
public class NoProviderAvailableException extends Exception {

    private final List<ProviderException> failures;
    private final long retryAfterMillis;

    NoProviderAvailableException(List<String> skipped, List<ProviderException> failures, long retryAfterMillis) {
        super("no provider available (skipped: " + skipped + ", failed: "
                + failures.stream().map(Throwable::getMessage).toList() + ")");
        this.failures = failures;
        this.retryAfterMillis = retryAfterMillis;
    }

    public List<ProviderException> failures() {
        return failures;
    }

    public long retryAfterMillis() {
        return retryAfterMillis;
    }

    public boolean allRateLimited() {
        return !failures.isEmpty() && failures.stream().allMatch(f -> f.kind() == ProviderException.Kind.RATE_LIMITED);
    }
}
