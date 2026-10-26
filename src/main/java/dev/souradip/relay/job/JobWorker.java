package dev.souradip.relay.job;

import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.budget.TokenBudget;
import dev.souradip.relay.budget.TokenEstimator;
import dev.souradip.relay.provider.Completion;
import dev.souradip.relay.provider.Message;
import dev.souradip.relay.provider.ModelRequest;
import dev.souradip.relay.provider.ProviderException;
import dev.souradip.relay.provider.Usage;
import dev.souradip.relay.routing.NoProviderAvailableException;
import dev.souradip.relay.routing.Router;
import dev.souradip.relay.tenant.Tenant;
import dev.souradip.relay.tenant.TenantService;
import dev.souradip.relay.webhook.WebhookService;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Pulls jobs and runs them. Each instance runs a few worker threads; any number of instances can
 * run side by side because every claim and every result write goes through the job's lease.
 *
 * <pre>
 *   claim (lease)  -> reserve budget -> call model -> SUCCEEDED, webhook queued
 *                       no budget       -> back in the queue, attempt not counted
 *                       provider busy   -> back in the queue with backoff
 *                       rejected (4xx)  -> DEAD with the reason
 *                       out of attempts -> DEAD with the last error
 * </pre>
 */
@Component
public class JobWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private final JobRepository jobs;
    private final TenantService tenants;
    private final TokenBudget budget;
    private final TokenEstimator estimator;
    private final Router router;
    private final WebhookService webhooks;
    private final RelayProperties.Jobs config;
    private final FairScheduler scheduler = new FairScheduler();
    private final ScheduledExecutorService leaseKeeper = Executors.newSingleThreadScheduledExecutor();
    private final String owner = "worker-" + UUID.randomUUID();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;
    private volatile RelayProperties.Scheduling scheduling;

    JobWorker(JobRepository jobs, TenantService tenants, TokenBudget budget, TokenEstimator estimator, Router router,
              WebhookService webhooks, RelayProperties properties) {
        this.jobs = jobs;
        this.tenants = tenants;
        this.budget = budget;
        this.estimator = estimator;
        this.router = router;
        this.webhooks = webhooks;
        this.config = properties.jobs();
        this.scheduling = config.scheduling();
    }

    @Override
    public void start() {
        running = true;
        if (!config.workersEnabled()) {
            return;
        }
        for (int i = 0; i < config.workers(); i++) {
            threads.add(Thread.ofVirtual().name("job-worker-" + i).start(this::loop));
        }
    }

    @Override
    public void stop() {
        running = false;
        long deadline = System.nanoTime() + config.lease().toNanos();
        for (Thread thread : threads) {
            try {
                thread.join(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Anything still held goes back now, so other instances can start on it straight away
        // instead of waiting for the lease to run out.
        long released = jobs.releaseAll(owner, Instant.now());
        if (released > 0) {
            log.info("released {} jobs on shutdown", released);
        }
        leaseKeeper.shutdownNow();
        threads.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 1;
    }

    private void loop() {
        while (running) {
            try {
                Optional<Job> job = claimNext();
                if (job.isPresent()) {
                    execute(job.get(), owner);
                } else {
                    Thread.sleep(config.idlePoll());
                }
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.warn("worker loop error, continuing: {}", e.toString());
                try {
                    Thread.sleep(config.idlePoll());
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
    }

    Optional<Job> claimNext() {
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(config.lease());
        if (scheduling == RelayProperties.Scheduling.FIFO) {
            return jobs.claim(null, owner, now, leaseUntil);
        }
        List<FairScheduler.Candidate> ready = new ArrayList<>();
        for (String tenantId : jobs.tenantsWithReadyJobs(now)) {
            Optional<Tenant> tenant = tenants.find(tenantId);
            // Soft cap: two workers can both see one free slot. A hard cap would need a
            // distributed semaphore; being one over for a moment is fine here.
            if (tenant.isPresent() && jobs.running(tenantId, now) < tenant.get().maxConcurrentJobs()) {
                ready.add(new FairScheduler.Candidate(tenantId, tenant.get().weight()));
            }
        }
        for (FairScheduler.Candidate candidate : scheduler.order(ready)) {
            Optional<Job> claimed = jobs.claim(candidate.tenantId(), owner, now, leaseUntil);
            if (claimed.isPresent()) {
                scheduler.charge(candidate);
                return claimed;
            }
        }
        return Optional.empty();
    }

    /** Package private and owner as a parameter so tests can play a second, slower worker. */
    void execute(Job job, String owner) {
        MDC.put("traceId", job.traceId());
        MDC.put("jobId", job.id());
        AtomicBoolean leaseLost = new AtomicBoolean();
        long every = Math.max(100, config.lease().toMillis() / 3);
        ScheduledFuture<?> keeper = leaseKeeper.scheduleAtFixedRate(() -> {
            if (!jobs.extendLease(job.id(), owner, Instant.now().plus(config.lease()))) {
                leaseLost.set(true);
            }
        }, every, every, TimeUnit.MILLISECONDS);
        try {
            run(job, owner);
        } finally {
            keeper.cancel(false);
            MDC.clear();
        }
    }

    private void run(Job job, String owner) {
        Instant now = Instant.now();
        if (job.attempts() > job.maxAttempts()) {
            // Only reachable through lease expiry: the job keeps killing or hanging its workers.
            finishDead(job, owner, "gave up after " + job.maxAttempts() + " attempts; last: " + job.lastError());
            return;
        }
        Optional<Tenant> tenant = tenants.find(job.tenantId());
        if (tenant.isEmpty()) {
            finishDead(job, owner, "tenant no longer exists");
            return;
        }

        List<Message> messages = List.of(new Message("user", job.prompt()));
        long inputTokens = estimator.inputTokens(messages);
        TokenBudget.Reservation reservation;
        switch (budget.tryReserve(tenant.get(), inputTokens + job.maxTokens())) {
            case TokenBudget.Decision.Granted granted -> reservation = granted.reservation();
            case TokenBudget.Decision.Denied denied -> {
                jobs.deferWithoutAttempt(job.id(), owner, now.plusMillis(denied.retryAfterMillis()),
                        "waiting for token budget", now);
                return;
            }
        }

        ModelRequest request = new ModelRequest(messages, job.maxTokens(), job.id() + ":" + job.attempts());
        try {
            Router.Opened<Completion> done = router.complete(request);
            Usage usage = done.value().usage() != null ? done.value().usage() : new Usage(inputTokens, job.maxTokens());
            budget.settle(reservation, usage.total());
            JobResult result = new JobResult(done.value().text(), done.lease().provider(), usage.inputTokens(), usage.outputTokens());
            if (jobs.succeed(job.id(), owner, result, Instant.now())) {
                jobs.find(job.id()).ifPresent(webhooks::jobFinished);
            } else {
                // Our lease ran out while we were working and another worker owns the job now.
                // Its result wins; ours is dropped. The tenant still sees one result and one webhook.
                log.info("job {} finished after its lease was taken over; discarding this result", job.id());
            }
        } catch (ProviderException rejected) {
            budget.settle(reservation, 0);
            finishDead(job, owner, "rejected by " + rejected.provider() + ": " + rejected.getMessage());
        } catch (NoProviderAvailableException e) {
            budget.settle(reservation, 0);
            String error = e.getMessage();
            if (job.attempts() >= job.maxAttempts()) {
                finishDead(job, owner, "gave up after " + job.attempts() + " attempts; last: " + error);
                return;
            }
            Duration wait = Backoff.forAttempt(job.attempts(), config.retryBase(), config.retryMax());
            if (e.allRateLimited()) {
                wait = wait.plusMillis(e.retryAfterMillis());
            }
            jobs.retryLater(job.id(), owner, Instant.now().plus(wait), error, Instant.now());
        }
    }

    private void finishDead(Job job, String owner, String reason) {
        log.warn("job {} is dead: {}", job.id(), reason);
        if (jobs.dead(job.id(), owner, reason, Instant.now())) {
            jobs.find(job.id()).ifPresent(webhooks::jobFinished);
        }
    }

    /** Switches the scheduling policy at run time, so both can be compared on the same running system. */
    void scheduling(RelayProperties.Scheduling scheduling) {
        this.scheduling = scheduling;
    }
}
