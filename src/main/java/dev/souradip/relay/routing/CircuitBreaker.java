package dev.souradip.relay.routing;

import dev.souradip.relay.RelayProperties;
import java.time.Clock;
import java.time.Instant;

/**
 * Stops sending traffic to a provider that is failing, and checks back later with a single probe.
 *
 * <pre>
 *   CLOSED  --failure rate over threshold in the last N calls-->  OPEN
 *   OPEN    --after openFor-->                                     HALF_OPEN (one probe allowed)
 *   HALF_OPEN --probe succeeds--> CLOSED      --probe fails--> OPEN
 * </pre>
 *
 * Without it, every request still waits for a provider that is down to time out before failing
 * over, so an outage at one provider becomes a latency spike for everyone.
 */
public class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final RelayProperties.Breaker config;
    private final Clock clock;
    private final boolean[] outcomes;
    private int next;
    private int recorded;
    private int failures;
    private State state = State.CLOSED;
    private Instant openUntil = Instant.MIN;
    private boolean probeInFlight;

    public CircuitBreaker(RelayProperties.Breaker config, Clock clock) {
        this.config = config;
        this.clock = clock;
        this.outcomes = new boolean[config.window()];
    }

    public synchronized boolean tryAcquire() {
        switch (state) {
            case CLOSED -> {
                return true;
            }
            case OPEN -> {
                if (clock.instant().isBefore(openUntil)) {
                    return false;
                }
                state = State.HALF_OPEN;
                probeInFlight = true;
                return true;
            }
            default -> {
                if (probeInFlight) {
                    return false;
                }
                probeInFlight = true;
                return true;
            }
        }
    }

    public synchronized void onSuccess() {
        if (state == State.HALF_OPEN) {
            reset();
            state = State.CLOSED;
            return;
        }
        record(false);
    }

    public synchronized void onFailure() {
        if (state == State.HALF_OPEN) {
            open();
            return;
        }
        record(true);
        if (recorded >= config.minimumCalls() && (double) failures / recorded >= config.failureRate()) {
            open();
        }
    }

    /** The call ended without telling us anything about the provider, for example the client left. */
    public synchronized void onIgnored() {
        if (state == State.HALF_OPEN) {
            probeInFlight = false;
        }
    }

    public synchronized State state() {
        if (state == State.OPEN && !clock.instant().isBefore(openUntil)) {
            return State.HALF_OPEN;
        }
        return state;
    }

    private void open() {
        state = State.OPEN;
        openUntil = clock.instant().plus(config.openFor());
        probeInFlight = false;
        reset();
    }

    private void record(boolean failed) {
        if (recorded == outcomes.length) {
            if (outcomes[next]) {
                failures--;
            }
        } else {
            recorded++;
        }
        outcomes[next] = failed;
        if (failed) {
            failures++;
        }
        next = (next + 1) % outcomes.length;
    }

    private void reset() {
        next = 0;
        recorded = 0;
        failures = 0;
    }
}
