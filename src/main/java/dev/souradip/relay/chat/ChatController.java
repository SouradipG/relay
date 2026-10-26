package dev.souradip.relay.chat;

import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.api.ApiException;
import dev.souradip.relay.budget.TokenBudget;
import dev.souradip.relay.budget.TokenEstimator;
import dev.souradip.relay.guard.PiiRedactor;
import dev.souradip.relay.provider.Message;
import dev.souradip.relay.provider.ModelRequest;
import dev.souradip.relay.provider.ProviderException;
import dev.souradip.relay.provider.TokenStream;
import dev.souradip.relay.routing.NoProviderAvailableException;
import dev.souradip.relay.routing.Router;
import dev.souradip.relay.tenant.Tenant;
import dev.souradip.relay.tenant.TenantService;
import jakarta.annotation.PreDestroy;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Synchronous chat over Server Sent Events.
 *
 * <p>Everything that can be refused is refused before the stream opens: authentication, the
 * instance being full or draining, the tenant's token budget, and every provider being down.
 * Those come back as ordinary JSON errors with a status code. Once the first byte of the stream
 * is sent the status is 200 and cannot change, so failures after that are reported as events.
 */
@RestController
class ChatController {

    private final TenantService tenants;
    private final TokenBudget budget;
    private final TokenEstimator estimator;
    private final PiiRedactor redactor;
    private final Router router;
    private final StreamRegistry registry;
    private final RelayProperties.Chat config;
    private final ExecutorService pumps = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeats = Executors.newScheduledThreadPool(1);

    ChatController(TenantService tenants, TokenBudget budget, TokenEstimator estimator, PiiRedactor redactor,
                   Router router, StreamRegistry registry, RelayProperties properties) {
        this.tenants = tenants;
        this.budget = budget;
        this.estimator = estimator;
        this.redactor = redactor;
        this.router = router;
        this.registry = registry;
        this.config = properties.chat();
    }

    @PostMapping(path = "/v1/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    ResponseEntity<SseEmitter> chat(@RequestHeader(value = "X-Api-Key", required = false) String apiKey, @Valid @RequestBody ChatRequest request) {
        Tenant tenant = tenants.authenticate(apiKey);
        if (registry.draining()) {
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "DRAINING", "instance is restarting", 1L);
        }
        if (!registry.hasRoom()) {
            throw atCapacity();
        }

        List<Message> messages = request.messages().stream()
                .map(m -> new Message(m.role(), redactor.redact(m.content())))
                .toList();
        long inputTokens = estimator.inputTokens(messages);
        TokenBudget.Reservation reservation = switch (budget.tryReserve(tenant, inputTokens + request.maxTokens())) {
            case TokenBudget.Decision.Granted granted -> granted.reservation();
            case TokenBudget.Decision.Denied denied -> throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "TOKEN_BUDGET",
                    "token budget for this minute is used up", Math.max(1, (denied.retryAfterMillis() + 999) / 1000));
        };

        Router.Opened<TokenStream> opened;
        try {
            opened = router.stream(new ModelRequest(messages, request.maxTokens(), UUID.randomUUID().toString()));
        } catch (ProviderException e) {
            budget.settle(reservation, 0);
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "REJECTED_BY_PROVIDER", e.getMessage());
        } catch (NoProviderAvailableException e) {
            budget.settle(reservation, 0);
            // Chat is interactive. Queueing it would only make the user wait for an error, so
            // say so straight away and let the client decide.
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "NO_PROVIDER",
                    e.allRateLimited() ? "all providers are rate limiting, retry shortly" : "no provider is available",
                    Math.max(1, e.retryAfterMillis() / 1000));
        }

        SseEmitter emitter = new SseEmitter(config.maxDuration().toMillis());
        ChatStream stream = new ChatStream(emitter, opened.value(), opened.lease(), budget, reservation, inputTokens, registry);
        if (!registry.tryRegister(stream)) {
            opened.value().close();
            opened.lease().abandoned();
            budget.settle(reservation, 0);
            throw atCapacity();
        }
        stream.startHeartbeat(heartbeats, config.heartbeat());
        pumps.submit(stream::pump);
        // Nginx and some other proxies buffer responses by default, and the user would get every
        // token at once at the end. This header turns that off for this response.
        return ResponseEntity.ok()
                .header("X-Accel-Buffering", "no")
                .header("Cache-Control", "no-cache")
                .body(emitter);
    }

    private static ApiException atCapacity() {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "AT_CAPACITY", "too many open streams on this instance", 1L);
    }

    @PreDestroy
    void shutdown() {
        heartbeats.shutdownNow();
        pumps.shutdown();
    }
}
