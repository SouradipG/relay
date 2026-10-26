package dev.souradip.relay.job;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * A unit of async work and its whole state machine in one document.
 *
 * <p>{@code leaseOwner} and {@code leaseUntil} are the visibility timeout: while the lease is
 * valid the job belongs to that worker. If the worker dies the lease runs out and any other
 * worker can claim the job. Every write a worker makes is conditional on still owning the lease,
 * so a worker that was only slow, not dead, cannot overwrite the result of the one that took over.
 */
@Document("jobs")
public record Job(
        @Id String id,
        String tenantId,
        String idempotencyKey,
        JobStatus status,
        String prompt,
        int maxTokens,
        int attempts,
        int maxAttempts,
        Instant nextAttemptAt,
        String leaseOwner,
        Instant leaseUntil,
        JobResult result,
        String lastError,
        String webhookUrl,
        String traceId,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {}
