package dev.souradip.relay.webhook;

import dev.souradip.relay.webhook.WebhookDelivery.DeliveryStatus;
import java.time.Instant;
import java.util.Optional;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

@Repository
public class WebhookRepository {

    private final MongoTemplate mongo;

    WebhookRepository(MongoTemplate mongo) {
        this.mongo = mongo;
        mongo.indexOps(WebhookDelivery.class).createIndex(
                new Index().on("status", Sort.Direction.ASC).on("nextAttemptAt", Sort.Direction.ASC));
    }

    /** @return false if a delivery for this job already exists */
    boolean enqueue(WebhookDelivery delivery) {
        try {
            mongo.insert(delivery);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public Optional<WebhookDelivery> find(String id) {
        return Optional.ofNullable(mongo.findById(id, WebhookDelivery.class));
    }

    Optional<WebhookDelivery> claimDue(String owner, Instant now, Instant leaseUntil) {
        Query due = Query.query(Criteria.where("status").is(DeliveryStatus.PENDING).and("nextAttemptAt").lte(now)
                        .orOperator(Criteria.where("leaseUntil").exists(false), Criteria.where("leaseUntil").lt(now)))
                .with(Sort.by("nextAttemptAt"));
        Update update = new Update().set("leaseOwner", owner).set("leaseUntil", leaseUntil).inc("attempts", 1);
        return Optional.ofNullable(mongo.findAndModify(due, update, FindAndModifyOptions.options().returnNew(true), WebhookDelivery.class));
    }

    void delivered(String id, int statusCode, Instant now) {
        mongo.updateFirst(byId(id), new Update().set("status", DeliveryStatus.DELIVERED).set("lastStatusCode", statusCode)
                .set("deliveredAt", now).unset("leaseOwner").unset("leaseUntil"), WebhookDelivery.class);
    }

    void retryLater(String id, Integer statusCode, String error, Instant nextAttemptAt) {
        mongo.updateFirst(byId(id), new Update().set("nextAttemptAt", nextAttemptAt).set("lastStatusCode", statusCode)
                .set("lastError", error).unset("leaseOwner").unset("leaseUntil"), WebhookDelivery.class);
    }

    void park(String id, Integer statusCode, String error) {
        mongo.updateFirst(byId(id), new Update().set("status", DeliveryStatus.PARKED).set("lastStatusCode", statusCode)
                .set("lastError", error).unset("leaseOwner").unset("leaseUntil"), WebhookDelivery.class);
    }

    boolean replay(String id, String tenantId, Instant now) {
        Query query = Query.query(Criteria.where("_id").is(id).and("tenantId").is(tenantId).and("status").is(DeliveryStatus.PARKED));
        return mongo.updateFirst(query, new Update().set("status", DeliveryStatus.PENDING).set("attempts", 0)
                .set("nextAttemptAt", now), WebhookDelivery.class).getModifiedCount() == 1;
    }

    private static Query byId(String id) {
        return Query.query(Criteria.where("_id").is(id));
    }
}
