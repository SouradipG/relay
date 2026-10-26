package dev.souradip.relay.routing;

import dev.souradip.relay.provider.ModelProvider;

/** A provider with its own breaker and concurrency limit. */
public record Route(ModelProvider provider, CircuitBreaker breaker, AdaptiveLimiter limiter) {

    public String name() {
        return provider.name();
    }
}
