package dev.souradip.relay.job;

import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.api.ApiException;
import dev.souradip.relay.budget.TokenEstimator;
import dev.souradip.relay.guard.PiiRedactor;
import dev.souradip.relay.provider.Message;
import dev.souradip.relay.tenant.Tenant;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class JobService {

    public record Submitted(Job job, boolean created) {}

    private final JobRepository jobs;
    private final PiiRedactor redactor;
    private final TokenEstimator estimator;
    private final RelayProperties.Jobs config;

    JobService(JobRepository jobs, PiiRedactor redactor, TokenEstimator estimator, RelayProperties properties) {
        this.jobs = jobs;
        this.redactor = redactor;
        this.estimator = estimator;
        this.config = properties.jobs();
    }

    public Submitted submit(Tenant tenant, JobRequest request, String idempotencyKey, String traceId) {
        String prompt = redactor.redact(request.prompt());
        long estimate = estimator.estimate(List.of(new Message("user", prompt)), request.maxTokens());
        if (estimate > tenant.tokensPerMinute()) {
            // Refuse now. Queued, it would wait for a budget that can never cover it.
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "REQUEST_TOO_LARGE",
                    "this job needs about " + estimate + " tokens, more than the tenant's whole per minute budget");
        }
        Instant now = Instant.now();
        Job job = new Job(UUID.randomUUID().toString(), tenant.id(), idempotencyKey, JobStatus.QUEUED, prompt,
                request.maxTokens(), 0, config.maxAttempts(), now, null, null, null, null, request.webhookUrl(),
                traceId, now, now, null);
        try {
            jobs.insert(job);
            return new Submitted(job, true);
        } catch (DuplicateKeyException e) {
            Job existing = jobs.findByKey(tenant.id(), idempotencyKey).orElseThrow();
            if (!existing.prompt().equals(prompt) || existing.maxTokens() != request.maxTokens()) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "IDEMPOTENCY_KEY_REUSED",
                        "this Idempotency-Key was already used for a different job");
            }
            return new Submitted(existing, false);
        }
    }

    public Job get(Tenant tenant, String id) {
        return jobs.find(id)
                .filter(j -> j.tenantId().equals(tenant.id()))
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", "no such job"));
    }
}
