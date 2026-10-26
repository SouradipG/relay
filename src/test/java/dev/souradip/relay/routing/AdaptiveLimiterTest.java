package dev.souradip.relay.routing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AdaptiveLimiterTest {

    @Test
    void refusesOnceTheLimitIsReached() {
        AdaptiveLimiter limiter = new AdaptiveLimiter(2, 10);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void growsSlowlyOnSuccessAndHalvesOnOverload() {
        AdaptiveLimiter limiter = new AdaptiveLimiter(8, 100);
        for (int i = 0; i < 80; i++) {
            limiter.tryAcquire();
            limiter.onSuccess();
        }
        assertThat(limiter.limit()).as("about +1 per limit's worth of successes").isBetween(14, 17);

        int before = limiter.limit();
        limiter.tryAcquire();
        limiter.onOverload();
        assertThat(limiter.limit()).isBetween(before / 2 - 1, before / 2 + 1);
    }

    @Test
    void aBurstOf429sHalvesOnceNotOncePerCall() {
        AdaptiveLimiter limiter = new AdaptiveLimiter(32, 64);
        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire();
        }
        for (int i = 0; i < 10; i++) {
            limiter.onOverload();
        }
        assertThat(limiter.limit()).isEqualTo(16);
        assertThat(limiter.inFlight()).isZero();
    }

    @Test
    void neverGoesBelowOneOrAboveMax() {
        AdaptiveLimiter limiter = new AdaptiveLimiter(1, 3);
        limiter.tryAcquire();
        limiter.onOverload();
        assertThat(limiter.limit()).isEqualTo(1);

        for (int i = 0; i < 100; i++) {
            limiter.tryAcquire();
            limiter.onSuccess();
        }
        assertThat(limiter.limit()).isEqualTo(3);
    }
}
