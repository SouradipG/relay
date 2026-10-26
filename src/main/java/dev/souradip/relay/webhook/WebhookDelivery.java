package dev.souradip.relay.webhook;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * One notification to a tenant's endpoint. The id is the job id, so a job that ran twice (a slow
 * worker and the one that took over) still produces one delivery.
 *
 * <p>The payload is fixed when the delivery is created, so every retry sends identical bytes and
 * a receiver can dedupe on the delivery id.
 */
@Document("webhook_deliveries")
public record WebhookDelivery(
        @Id String id,
        String tenantId,
        String url,
        String payload,
        DeliveryStatus status,
        int attempts,
        Instant nextAttemptAt,
        String leaseOwner,
        Instant leaseUntil,
        Integer lastStatusCode,
        String lastError,
        Instant createdAt,
        Instant deliveredAt) {

    public enum DeliveryStatus {
        PENDING,
        DELIVERED,
        /** Gave up after the maximum attempts. Waits for a manual replay. */
        PARKED
    }
}
