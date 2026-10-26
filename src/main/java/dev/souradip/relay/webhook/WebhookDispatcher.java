package dev.souradip.relay.webhook;

import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.tenant.Tenant;
import dev.souradip.relay.tenant.TenantService;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Delivers webhooks at least once, with backoff, and parks them after the last attempt.
 *
 * <p>Every instance runs this. Claims are leased with findAndModify, so instances share the work
 * instead of each sending every delivery, which is what a plain {@code @Scheduled} job would do
 * once there are N instances.
 */
@Component
class WebhookDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);

    private final WebhookRepository deliveries;
    private final TenantService tenants;
    private final RelayProperties.Webhooks config;
    private final HttpClient http;
    private final String owner = "dispatcher-" + UUID.randomUUID();

    WebhookDispatcher(WebhookRepository deliveries, TenantService tenants, RelayProperties properties) {
        this.deliveries = deliveries;
        this.tenants = tenants;
        this.config = properties.webhooks();
        this.http = HttpClient.newBuilder().connectTimeout(config.timeout()).build();
    }

    @Scheduled(fixedDelayString = "${relay.webhooks.poll}")
    void run() {
        if (!config.dispatcherEnabled()) {
            return;
        }
        try {
            for (int i = 0; i < 100; i++) {
                Instant now = Instant.now();
                Optional<WebhookDelivery> due = deliveries.claimDue(owner, now, now.plus(config.timeout().multipliedBy(3)));
                if (due.isEmpty()) {
                    return;
                }
                deliver(due.get());
            }
        } catch (RuntimeException e) {
            log.warn("webhook pass failed, will retry: {}", e.toString());
        }
    }

    void deliver(WebhookDelivery delivery) {
        Optional<Tenant> tenant = tenants.find(delivery.tenantId());
        if (tenant.isEmpty()) {
            deliveries.park(delivery.id(), null, "tenant no longer exists");
            return;
        }
        Integer status = null;
        String error;
        try {
            long timestamp = Instant.now().getEpochSecond();
            HttpRequest request = HttpRequest.newBuilder(URI.create(delivery.url()))
                    .timeout(config.timeout())
                    .header("Content-Type", "application/json")
                    .header("Relay-Delivery-Id", delivery.id())
                    .header("Relay-Signature", WebhookSigner.sign(tenant.get().webhookSecret(), timestamp, delivery.payload()))
                    .POST(HttpRequest.BodyPublishers.ofString(delivery.payload()))
                    .build();
            status = http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
            if (status >= 200 && status < 300) {
                deliveries.delivered(delivery.id(), status, Instant.now());
                return;
            }
            error = "receiver answered " + status;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = "interrupted";
        } catch (Exception e) {
            error = e.toString();
        }

        if (delivery.attempts() >= config.maxAttempts()) {
            log.warn("parking webhook {} after {} attempts: {}", delivery.id(), delivery.attempts(), error);
            deliveries.park(delivery.id(), status, error);
            return;
        }
        long ceiling = Math.min(config.retryMax().toMillis(), config.retryBase().toMillis() << Math.min(20, delivery.attempts() - 1));
        Duration wait = Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceiling / 2, ceiling + 1));
        deliveries.retryLater(delivery.id(), status, error, Instant.now().plus(wait));
    }
}
