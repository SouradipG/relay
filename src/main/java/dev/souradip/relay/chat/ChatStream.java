package dev.souradip.relay.chat;

import dev.souradip.relay.budget.TokenBudget;
import dev.souradip.relay.provider.ProviderException;
import dev.souradip.relay.provider.TokenStream;
import dev.souradip.relay.provider.Usage;
import dev.souradip.relay.routing.Lease;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Copies one upstream stream to one client, and makes sure every way it can end is accounted for.
 *
 * <pre>
 *   upstream finishes        -> event: done, settle with reported usage
 *   upstream breaks midway   -> event: error with how many chunks were delivered. No silent
 *                               restart: that would repeat the start of the answer and bill twice.
 *   client disconnects       -> close upstream so the provider stops generating, settle with
 *                               what was actually produced
 *   instance shutting down   -> event: error SERVER_RESTARTING after the drain deadline
 * </pre>
 *
 * Heartbeat comments go out while the model is thinking. Load balancers close connections that
 * are idle for too long (60 s by default on an AWS ALB), and a long pause before the first token
 * is normal for large prompts.
 */
class ChatStream {

    private static final Logger log = LoggerFactory.getLogger(ChatStream.class);

    private final SseEmitter emitter;
    private final TokenStream upstream;
    private final Lease lease;
    private final TokenBudget budget;
    private final TokenBudget.Reservation reservation;
    private final long inputTokens;
    private final StreamRegistry registry;
    private final AtomicBoolean finished = new AtomicBoolean();
    private volatile boolean clientGone;
    private volatile boolean shuttingDown;
    private ScheduledFuture<?> heartbeat;

    ChatStream(SseEmitter emitter, TokenStream upstream, Lease lease, TokenBudget budget,
               TokenBudget.Reservation reservation, long inputTokens, StreamRegistry registry) {
        this.emitter = emitter;
        this.upstream = upstream;
        this.lease = lease;
        this.budget = budget;
        this.reservation = reservation;
        this.inputTokens = inputTokens;
        this.registry = registry;
        emitter.onError(e -> clientLeft());
        emitter.onTimeout(this::clientLeft);
    }

    void startHeartbeat(ScheduledExecutorService scheduler, Duration every) {
        heartbeat = scheduler.scheduleAtFixedRate(() -> {
            try {
                emitter.send(SseEmitter.event().comment("keep-alive"));
            } catch (IOException | IllegalStateException e) {
                clientLeft();
            }
        }, every.toMillis(), every.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Runs on its own virtual thread until the stream ends one way or another. */
    void pump() {
        try {
            String text;
            while ((text = upstream.next()) != null) {
                if (!send("token", Map.of("text", text))) {
                    clientLeft();
                    return;
                }
            }
            Usage usage = usageOrCounted();
            lease.succeeded();
            send("done", Map.of("provider", lease.provider(), "inputTokens", usage.inputTokens(),
                    "outputTokens", usage.outputTokens()));
            finish(usage.total());
            emitter.complete();
        } catch (ProviderException e) {
            if (clientGone || shuttingDown) {
                // We closed the upstream ourselves. Not the provider's fault.
                lease.abandoned();
                finish(usageOrCounted().total());
                if (shuttingDown) {
                    emitter.complete();
                }
                return;
            }
            log.warn("stream from {} broke after {} chunks: {}", lease.provider(), upstream.received(), e.getMessage());
            lease.failed(e);
            send("error", Map.of("code", "UPSTREAM_BROKEN", "delivered", upstream.received(),
                    "message", "the model stopped partway; the text so far is all there is, retry to get a new answer"));
            finish(usageOrCounted().total());
            emitter.complete();
        } catch (RuntimeException e) {
            log.error("stream failed", e);
            lease.abandoned();
            finish(usageOrCounted().total());
            emitter.completeWithError(e);
        }
    }

    void clientLeft() {
        if (finished.get()) {
            return;
        }
        clientGone = true;
        // Closing upstream makes the pump's blocking read fail, and it settles from there.
        upstream.close();
    }

    void abortForShutdown() {
        if (finished.get()) {
            return;
        }
        shuttingDown = true;
        send("error", Map.of("code", "SERVER_RESTARTING", "delivered", upstream.received(),
                "message", "this instance is restarting; retry and you will reach another one"));
        upstream.close();
    }

    private boolean send(String event, Object data) {
        try {
            emitter.send(SseEmitter.event().name(event).data(data, MediaType.APPLICATION_JSON));
            return true;
        } catch (IOException | IllegalStateException e) {
            return false;
        }
    }

    private Usage usageOrCounted() {
        Usage reported = upstream.usage();
        // A cut stream usually never reaches the usage chunk. Count what was produced instead,
        // about one token per chunk, so the tenant pays for what they got and nothing more.
        return reported != null ? reported : new Usage(inputTokens, upstream.received());
    }

    private void finish(long actualTokens) {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        if (heartbeat != null) {
            heartbeat.cancel(false);
        }
        upstream.close();
        budget.settle(reservation, actualTokens);
        registry.unregister(this);
    }
}
