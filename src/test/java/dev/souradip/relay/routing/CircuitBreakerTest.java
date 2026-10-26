package dev.souradip.relay.routing;

import static org.assertj.core.api.Assertions.assertThat;

import dev.souradip.relay.RelayProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

    private final MutableClock clock = new MutableClock();
    private final CircuitBreaker breaker = new CircuitBreaker(
            new RelayProperties.Breaker(10, 5, 0.5, Duration.ofSeconds(10)), clock);

    @Test
    void staysClosedUntilEnoughCallsFail() {
        for (int i = 0; i < 4; i++) {
            breaker.onFailure();
        }
        assertThat(breaker.state()).as("four calls are too few to judge").isEqualTo(CircuitBreaker.State.CLOSED);

        breaker.onFailure();
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void occasionalFailuresDoNotOpenIt() {
        for (int i = 0; i < 30; i++) {
            if (i % 3 == 0) {
                breaker.onFailure();
            } else {
                breaker.onSuccess();
            }
        }
        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void halfOpenAllowsOneProbeAndClosesOnSuccess() {
        open();
        clock.advance(Duration.ofSeconds(11));

        assertThat(breaker.tryAcquire()).isTrue();
        assertThat(breaker.tryAcquire()).as("only one probe at a time").isFalse();
        breaker.onSuccess();

        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(breaker.tryAcquire()).isTrue();
    }

    @Test
    void failedProbeOpensItAgain() {
        open();
        clock.advance(Duration.ofSeconds(11));
        breaker.tryAcquire();

        breaker.onFailure();

        assertThat(breaker.state()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void probeWhoseCallerLeftFreesTheSlot() {
        open();
        clock.advance(Duration.ofSeconds(11));
        breaker.tryAcquire();

        breaker.onIgnored();

        assertThat(breaker.tryAcquire()).isTrue();
    }

    private void open() {
        for (int i = 0; i < 5; i++) {
            breaker.onFailure();
        }
    }

    static final class MutableClock extends Clock {

        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
