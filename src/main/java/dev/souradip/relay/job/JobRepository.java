package dev.souradip.relay.job;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

@Repository
public class JobRepository {

    private final MongoTemplate mongo;

    JobRepository(MongoTemplate mongo) {
        this.mongo = mongo;
        var indexes = mongo.indexOps(Job.class);
        indexes.createIndex(new Index().on("status", Sort.Direction.ASC).on("nextAttemptAt", Sort.Direction.ASC));
        indexes.createIndex(new Index().on("tenantId", Sort.Direction.ASC).on("status", Sort.Direction.ASC)
                .on("nextAttemptAt", Sort.Direction.ASC));
        // One job per tenant and idempotency key. Partial, so jobs submitted without a key
        // do not all collide on a missing value.
        indexes.createIndex(new Index().on("tenantId", Sort.Direction.ASC).on("idempotencyKey", Sort.Direction.ASC)
                .unique().partial(PartialIndexFilter.of(Criteria.where("idempotencyKey").exists(true))));
    }

    void insert(Job job) {
        mongo.insert(job);
    }

    public Optional<Job> find(String id) {
        return Optional.ofNullable(mongo.findById(id, Job.class));
    }

    Optional<Job> findByKey(String tenantId, String idempotencyKey) {
        return Optional.ofNullable(mongo.findOne(
                Query.query(Criteria.where("tenantId").is(tenantId).and("idempotencyKey").is(idempotencyKey)), Job.class));
    }

    /** Ready means queued and due, or running under a lease that has run out (its worker is gone). */
    private static Criteria ready(Instant now) {
        return new Criteria().orOperator(
                Criteria.where("status").is(JobStatus.QUEUED).and("nextAttemptAt").lte(now),
                Criteria.where("status").is(JobStatus.RUNNING).and("leaseUntil").lt(now));
    }

    List<String> tenantsWithReadyJobs(Instant now) {
        return mongo.findDistinct(Query.query(ready(now)), "tenantId", Job.class, String.class);
    }

    long running(String tenantId, Instant now) {
        return mongo.count(Query.query(Criteria.where("tenantId").is(tenantId)
                .and("status").is(JobStatus.RUNNING).and("leaseUntil").gte(now)), Job.class);
    }

    /**
     * Atomically takes the oldest ready job, of one tenant or of anyone when {@code tenantId} is
     * null. findAndModify is a single document operation, so two workers can never claim the
     * same job.
     */
    Optional<Job> claim(String tenantId, String owner, Instant now, Instant leaseUntil) {
        Criteria criteria = tenantId == null ? ready(now) : new Criteria().andOperator(Criteria.where("tenantId").is(tenantId), ready(now));
        Query query = Query.query(criteria).with(Sort.by("nextAttemptAt", "createdAt"));
        Update update = new Update()
                .set("status", JobStatus.RUNNING)
                .set("leaseOwner", owner)
                .set("leaseUntil", leaseUntil)
                .set("updatedAt", now)
                .inc("attempts", 1);
        return Optional.ofNullable(mongo.findAndModify(query, update, FindAndModifyOptions.options().returnNew(true), Job.class));
    }

    boolean extendLease(String id, String owner, Instant until) {
        return updateOwned(id, owner, new Update().set("leaseUntil", until));
    }

    boolean succeed(String id, String owner, JobResult result, Instant now) {
        return updateOwned(id, owner, new Update()
                .set("status", JobStatus.SUCCEEDED).set("result", result).set("completedAt", now)
                .set("updatedAt", now).unset("leaseOwner").unset("leaseUntil"));
    }

    boolean retryLater(String id, String owner, Instant nextAttemptAt, String error, Instant now) {
        return updateOwned(id, owner, new Update()
                .set("status", JobStatus.QUEUED).set("nextAttemptAt", nextAttemptAt).set("lastError", error)
                .set("updatedAt", now).unset("leaseOwner").unset("leaseUntil"));
    }

    /** Puts a job back without counting the attempt, for waits that are not the job's fault. */
    boolean deferWithoutAttempt(String id, String owner, Instant nextAttemptAt, String reason, Instant now) {
        return updateOwned(id, owner, new Update()
                .set("status", JobStatus.QUEUED).set("nextAttemptAt", nextAttemptAt).set("lastError", reason)
                .set("updatedAt", now).inc("attempts", -1).unset("leaseOwner").unset("leaseUntil"));
    }

    boolean dead(String id, String owner, String error, Instant now) {
        return updateOwned(id, owner, new Update()
                .set("status", JobStatus.DEAD).set("lastError", error).set("completedAt", now)
                .set("updatedAt", now).unset("leaseOwner").unset("leaseUntil"));
    }

    /** On shutdown: give back everything this worker holds so others can start on it now. */
    long releaseAll(String owner, Instant now) {
        return mongo.updateMulti(
                Query.query(Criteria.where("leaseOwner").is(owner).and("status").is(JobStatus.RUNNING)),
                new Update().set("status", JobStatus.QUEUED).set("nextAttemptAt", now).set("updatedAt", now)
                        .inc("attempts", -1).unset("leaseOwner").unset("leaseUntil"),
                Job.class).getModifiedCount();
    }

    long count(JobStatus status) {
        return mongo.count(Query.query(Criteria.where("status").is(status)), Job.class);
    }

    private boolean updateOwned(String id, String owner, Update update) {
        Query owned = Query.query(Criteria.where("_id").is(id).and("leaseOwner").is(owner).and("status").is(JobStatus.RUNNING));
        return mongo.updateFirst(owned, update, Job.class).getModifiedCount() == 1;
    }
}
