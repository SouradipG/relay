package dev.souradip.relay.routing;

import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.provider.Completion;
import dev.souradip.relay.provider.ModelProvider;
import dev.souradip.relay.provider.ModelRequest;
import dev.souradip.relay.provider.OpenAiCompatibleProvider;
import dev.souradip.relay.provider.ProviderException;
import dev.souradip.relay.provider.SpringAiProvider;
import dev.souradip.relay.provider.TokenStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Picks a provider for each call and fails over to the next one.
 *
 * <p>Providers are tried in the configured order. One is skipped when its breaker is open or it
 * has no spare concurrency. A failure moves on to the next provider, except when the provider
 * rejected the request itself (a 400): every provider would reject it, and retrying would only
 * spend money.
 *
 * <p>Failover is only possible before the first token. Once a stream has started, switching
 * providers would restart the answer from the beginning, and the client would see two answers
 * glued together and be billed for both.
 */
@Component
public class Router {

    private static final Logger log = LoggerFactory.getLogger(Router.class);

    public record Opened<T>(T value, Lease lease) {}

    private final List<Route> routes = new CopyOnWriteArrayList<>();
    private final List<Integer> initialLimits = new ArrayList<>();
    private final List<Integer> maxLimits = new ArrayList<>();
    private final RelayProperties.Breaker breakerConfig;

    Router(RelayProperties properties, JsonMapper json, ObjectProvider<ChatModel> springAiModel) {
        this.breakerConfig = properties.breaker();
        for (RelayProperties.Provider provider : properties.providers()) {
            add(new OpenAiCompatibleProvider(provider, json), provider.initialConcurrency(), provider.maxConcurrency(), properties);
        }
        springAiModel.ifAvailable(model -> add(new SpringAiProvider("spring-ai", model), 8, 64, properties));
    }

    private void add(ModelProvider provider, int initial, int max, RelayProperties properties) {
        routes.add(new Route(provider, new CircuitBreaker(properties.breaker(), Clock.systemUTC()), new AdaptiveLimiter(initial, max)));
        initialLimits.add(initial);
        maxLimits.add(max);
    }

    /** Forgets everything learned about provider health. For tests and for an operator after an incident. */
    public synchronized void resetHealth() {
        for (int i = 0; i < routes.size(); i++) {
            Route route = routes.get(i);
            routes.set(i, new Route(route.provider(), new CircuitBreaker(breakerConfig, Clock.systemUTC()),
                    new AdaptiveLimiter(initialLimits.get(i), maxLimits.get(i))));
        }
    }

    public Opened<TokenStream> stream(ModelRequest request) throws ProviderException, NoProviderAvailableException {
        return route(request, true, (provider, r) -> provider.stream(r));
    }

    /** For a blocking call the outcome is known straight away, so the lease is settled here. */
    public Opened<Completion> complete(ModelRequest request) throws ProviderException, NoProviderAvailableException {
        return route(request, false, (provider, r) -> provider.complete(r));
    }

    public List<Route> routes() {
        return List.copyOf(routes);
    }

    public Optional<Route> route(String name) {
        return routes.stream().filter(r -> r.name().equals(name)).findFirst();
    }

    private interface Call<T> {
        T run(ModelProvider provider, ModelRequest request) throws ProviderException;
    }

    private <T> Opened<T> route(ModelRequest request, boolean holdLease, Call<T> call)
            throws ProviderException, NoProviderAvailableException {
        List<String> skipped = new ArrayList<>();
        List<ProviderException> failures = new ArrayList<>();
        long retryAfter = Long.MAX_VALUE;

        for (Route route : routes) {
            if (!route.limiter().tryAcquire()) {
                skipped.add(route.name() + " (at concurrency limit " + route.limiter().limit() + ")");
                continue;
            }
            if (!route.breaker().tryAcquire()) {
                route.limiter().onOther();
                skipped.add(route.name() + " (breaker open)");
                continue;
            }
            Lease lease = new Lease(route);
            try {
                T value = call.run(route.provider(), request);
                if (!holdLease) {
                    lease.succeeded();
                }
                return new Opened<>(value, lease);
            } catch (ProviderException e) {
                lease.failed(e);
                if (!e.retryable()) {
                    throw e;
                }
                log.info("provider {} failed, trying the next one: {}", route.name(), e.getMessage());
                failures.add(e);
                if (e.retryAfterMillis() > 0) {
                    retryAfter = Math.min(retryAfter, e.retryAfterMillis());
                }
            }
        }
        throw new NoProviderAvailableException(skipped, failures, retryAfter == Long.MAX_VALUE ? 1000 : retryAfter);
    }
}
