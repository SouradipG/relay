package dev.souradip.relay.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.provider.Message;
import dev.souradip.relay.provider.ModelRequest;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class FailoverTest extends IntegrationTestBase {

    /**
     * Provider alpha starts returning 429 for every call. Its breaker opens and traffic moves to
     * beta without every call first waiting on alpha. When alpha recovers, a probe finds out and
     * traffic comes back.
     */
    @Test
    void breakerShiftsTrafficAwayAndBackAgain() throws Exception {
        ALPHA.rateLimitedShare(1.0);

        for (int i = 0; i < 40; i++) {
            Router.Opened<?> done = router.complete(request());
            assertThat(done.lease().provider()).isIn("alpha", "beta");
        }

        Route alpha = router.route("alpha").orElseThrow();
        assertThat(alpha.breaker().state()).isNotEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(ALPHA.requests()).as("once open, alpha stops being asked").isLessThan(20);
        assertThat(BETA.completed()).isGreaterThan(20);

        ALPHA.rateLimitedShare(0);
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            router.complete(request());
            assertThat(alpha.breaker().state()).isEqualTo(CircuitBreaker.State.CLOSED);
        });
        assertThat(router.complete(request()).lease().provider()).isEqualTo("alpha");
    }

    @Test
    void rateLimitsShrinkTheConcurrencyLimit() throws Exception {
        Route alpha = router.route("alpha").orElseThrow();
        int before = alpha.limiter().limit();
        ALPHA.rateLimitedShare(1.0);

        router.complete(request());

        assertThat(alpha.limiter().limit()).isLessThan(before);
        assertThat(alpha.limiter().inFlight()).isZero();
    }

    private static ModelRequest request() {
        return new ModelRequest(List.of(new Message("user", "hi")), 10, UUID.randomUUID().toString());
    }
}
