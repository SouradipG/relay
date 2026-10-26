package dev.souradip.relay.job;

import dev.souradip.relay.tenant.Tenant;
import dev.souradip.relay.tenant.TenantService;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * Asynchronous jobs: accept, return 202 and a job id straight away, do the work later.
 *
 * <p>The right choice when the caller does not need the answer to carry on, or when the work is
 * long or unbounded. The request never holds a thread or a connection while a model thinks, a
 * provider outage becomes a delay instead of an error, and the queue absorbs bursts the
 * providers could not take.
 */
@RestController
class JobController {

    private final TenantService tenants;
    private final JobService jobs;

    JobController(TenantService tenants, JobService jobs) {
        this.tenants = tenants;
        this.jobs = jobs;
    }

    @PostMapping("/v1/jobs")
    ResponseEntity<JobView> submit(@RequestHeader(value = "X-Api-Key", required = false) String apiKey,
                                   @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                   @RequestHeader(value = "traceparent", required = false) String traceparent,
                                   @Valid @RequestBody JobRequest request) {
        Tenant tenant = tenants.authenticate(apiKey);
        JobService.Submitted submitted = jobs.submit(tenant, request, idempotencyKey, traceId(traceparent));
        return ResponseEntity.accepted()
                .location(URI.create("/v1/jobs/" + submitted.job().id()))
                .header("Idempotent-Replayed", String.valueOf(!submitted.created()))
                .body(JobView.of(submitted.job()));
    }

    @GetMapping("/v1/jobs/{id}")
    JobView get(@RequestHeader(value = "X-Api-Key", required = false) String apiKey, @PathVariable String id) {
        return JobView.of(jobs.get(tenants.authenticate(apiKey), id));
    }

    /**
     * Trace context does not cross a queue by itself. The W3C trace id from the request is stored
     * on the job and put back into the worker's logging context, so both halves of the request
     * can be found under one id.
     */
    private static String traceId(String traceparent) {
        if (traceparent != null) {
            String[] parts = traceparent.split("-");
            if (parts.length == 4 && parts[1].length() == 32) {
                return parts[1];
            }
        }
        return UUID.randomUUID().toString().replace("-", "");
    }
}
