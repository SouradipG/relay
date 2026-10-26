package dev.souradip.relay.routing;

import dev.souradip.relay.provider.ProviderException;

/**
 * The right to use one provider for one call. Report how the call went exactly once, so the
 * provider's breaker and limiter learn from it and the concurrency slot is given back.
 */
public final class Lease {

    private final Route route;
    private boolean released;

    Lease(Route route) {
        this.route = route;
    }

    public String provider() {
        return route.name();
    }

    public synchronized void succeeded() {
        if (release()) {
            route.breaker().onSuccess();
            route.limiter().onSuccess();
        }
    }

    public synchronized void failed(ProviderException e) {
        if (!release()) {
            return;
        }
        if (e.countsAgainstProvider()) {
            route.breaker().onFailure();
        } else {
            route.breaker().onIgnored();
        }
        if (e.kind() == ProviderException.Kind.RATE_LIMITED) {
            route.limiter().onOverload();
        } else {
            route.limiter().onOther();
        }
    }

    /** The caller went away. Says nothing about the provider's health. */
    public synchronized void abandoned() {
        if (release()) {
            route.breaker().onIgnored();
            route.limiter().onOther();
        }
    }

    private boolean release() {
        if (released) {
            return false;
        }
        released = true;
        return true;
    }
}
