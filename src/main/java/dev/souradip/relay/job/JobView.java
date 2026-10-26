package dev.souradip.relay.job;

import java.time.Instant;

public record JobView(String id, JobStatus status, int attempts, JobResult result, String lastError,
                      Instant createdAt, Instant completedAt) {

    static JobView of(Job job) {
        return new JobView(job.id(), job.status(), job.attempts(), job.result(),
                job.status() == JobStatus.SUCCEEDED ? null : job.lastError(), job.createdAt(), job.completedAt());
    }
}
