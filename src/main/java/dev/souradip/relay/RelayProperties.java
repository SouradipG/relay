package dev.souradip.relay;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("relay")
public record RelayProperties(
        String adminToken,
        List<Provider> providers,
        Breaker breaker,
        Chat chat,
        Jobs jobs,
        Webhooks webhooks) {

    /**
     * One upstream model endpoint that speaks the OpenAI chat completions protocol. Order in the
     * list is preference order: the router tries the first healthy one with spare capacity.
     */
    public record Provider(String name, String baseUrl, String apiKey, String model, int initialConcurrency,
                           int maxConcurrency, Duration connectTimeout, Duration firstByteTimeout,
                           Duration idleTimeout) {}

    public record Breaker(int window, int minimumCalls, double failureRate, Duration openFor) {}

    public record Chat(int maxStreams, Duration heartbeat, Duration maxDuration, Duration drainTimeout) {}

    public record Jobs(boolean workersEnabled, int workers, Scheduling scheduling, Duration lease, int maxAttempts,
                       Duration retryBase, Duration retryMax, Duration idlePoll) {}

    public enum Scheduling { FAIR, FIFO }

    public record Webhooks(boolean dispatcherEnabled, Duration poll, Duration timeout, int maxAttempts,
                           Duration retryBase, Duration retryMax) {}
}
