package dev.souradip.relay.job;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with full jitter: a random wait between zero and base * 2^attempt, capped.
 *
 * <p>Without the jitter, every job that failed in the same outage retries at the same moment, and
 * the provider that just recovered gets hit by all of them at once and goes down again.
 */
final class Backoff {

    private Backoff() {}

    static Duration forAttempt(int attempt, Duration base, Duration max) {
        long ceiling = base.toMillis() << Math.min(20, Math.max(0, attempt - 1));
        ceiling = Math.min(max.toMillis(), ceiling);
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceiling + 1));
    }
}
