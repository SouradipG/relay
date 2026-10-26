package dev.souradip.relay.routing;

/**
 * How many calls may be in flight to one provider, discovered at run time.
 *
 * <p>Providers rarely publish a usable concurrency number, and the real one moves with their load.
 * This uses additive increase, multiplicative decrease, the same idea TCP uses for congestion:
 * every success raises the limit a little, every 429 halves it. The limit settles just under
 * what the provider will take, instead of the gateway hammering it into returning 429 for
 * everything.
 */
public class AdaptiveLimiter {

    private static final long DECREASE_COOLDOWN_NANOS = 100_000_000L;

    private final int max;
    private double limit;
    private int inFlight;
    private long lastDecrease;

    public AdaptiveLimiter(int initial, int max) {
        this.max = max;
        this.limit = Math.min(initial, max);
    }

    public synchronized boolean tryAcquire() {
        if (inFlight >= (int) limit) {
            return false;
        }
        inFlight++;
        return true;
    }

    public synchronized void onSuccess() {
        inFlight--;
        limit = Math.min(max, limit + 1.0 / limit);
    }

    public synchronized void onOverload() {
        inFlight--;
        long now = System.nanoTime();
        // Many calls in flight see the same overload moment. Halving once per burst rather than
        // once per 429 stops the limit collapsing to 1 on a single bad second.
        if (now - lastDecrease > DECREASE_COOLDOWN_NANOS) {
            limit = Math.max(1, limit / 2);
            lastDecrease = now;
        }
    }

    public synchronized void onOther() {
        inFlight--;
    }

    public synchronized int limit() {
        return (int) limit;
    }

    public synchronized int inFlight() {
        return inFlight;
    }
}
