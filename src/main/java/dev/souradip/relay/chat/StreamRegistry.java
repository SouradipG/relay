package dev.souradip.relay.chat;

import dev.souradip.relay.RelayProperties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Tracks open streams and drains them on shutdown.
 *
 * <p>A rolling deploy sends SIGTERM and kills the task once the stop timeout runs out (30 s by
 * default on ECS). Without draining, every stream on that instance is cut mid answer. On stop
 * this refuses new streams, so the load balancer's retry lands on another instance, and waits
 * for the open ones to finish. Streams still running at the deadline get a clean
 * {@code SERVER_RESTARTING} event instead of a dropped connection.
 *
 * <p>It stops before the web server's own graceful shutdown, which would otherwise wait on
 * these requests without knowing they are streams.
 */
@Component
public class StreamRegistry implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StreamRegistry.class);

    private final Set<ChatStream> open = ConcurrentHashMap.newKeySet();
    private final RelayProperties.Chat config;
    private volatile boolean running;
    private volatile boolean draining;

    StreamRegistry(RelayProperties properties) {
        this.config = properties.chat();
    }

    /** @return false when the instance is full or shutting down */
    synchronized boolean tryRegister(ChatStream stream) {
        if (draining || open.size() >= config.maxStreams()) {
            return false;
        }
        open.add(stream);
        return true;
    }

    /** A cheap early check, so a full instance does not open an upstream call it cannot serve. */
    boolean hasRoom() {
        return open.size() < config.maxStreams();
    }

    void unregister(ChatStream stream) {
        open.remove(stream);
    }

    public int openStreams() {
        return open.size();
    }

    public boolean draining() {
        return draining;
    }

    @Override
    public void start() {
        running = true;
        draining = false;
    }

    @Override
    public void stop() {
        synchronized (this) {
            draining = true;
        }
        long deadline = System.nanoTime() + config.drainTimeout().toNanos();
        log.info("draining {} open streams", open.size());
        while (!open.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        for (ChatStream stream : open) {
            stream.abortForShutdown();
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Higher phases stop first. The web server's graceful shutdown runs at DEFAULT_PHASE - 2048.
        return SmartLifecycle.DEFAULT_PHASE - 1;
    }
}
