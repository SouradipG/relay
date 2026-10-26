package dev.souradip.relay.job;

public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    /** Gave up: the request was rejected, or it failed on every attempt. Kept with the reason. */
    DEAD
}
