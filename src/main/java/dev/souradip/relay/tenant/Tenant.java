package dev.souradip.relay.tenant;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * A team or product that calls the gateway.
 *
 * @param weight            share of job worker time relative to other tenants with work waiting
 * @param tokensPerMinute   token budget, refilled continuously
 * @param maxConcurrentJobs how many of this tenant's jobs may run at once across all workers
 */
@Document("tenants")
public record Tenant(@Id String id, String name, String apiKeyHash, String webhookSecret, int weight,
                     long tokensPerMinute, int maxConcurrentJobs, Instant createdAt) {}
