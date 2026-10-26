package dev.souradip.relay.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.RelayProperties;
import dev.souradip.relay.tenant.Tenant;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 * The same flood as {@link FairnessTest} with a plain FIFO queue, to show what fairness buys.
 * The policy is switched on the running workers: a second application context would bring a
 * second set of workers polling the same database.
 */
class FifoContrastTest extends IntegrationTestBase {

    @Autowired
    JobService jobService;

    @Autowired
    JobWorker worker;

    @BeforeEach
    void fifo() {
        worker.scheduling(RelayProperties.Scheduling.FIFO);
    }

    @AfterEach
    void fair() {
        worker.scheduling(RelayProperties.Scheduling.FAIR);
    }

    @Test
    void withFifoTheSmallTenantWaitsForTheWholeFlood() {
        Tenant big = tenants.find(tenant("batch", 1, 10_000_000, 4).id()).orElseThrow();
        Tenant small = tenants.find(tenant("interactive", 1, 10_000_000, 4).id()).orElseThrow();
        ALPHA.tokenDelay(Duration.ofMillis(2));

        for (int i = 0; i < 200; i++) {
            jobService.submit(big, new JobRequest("bulk " + i, 20, null), null, "t");
        }
        for (int i = 0; i < 10; i++) {
            jobService.submit(small, new JobRequest("urgent " + i, 20, null), null, "t");
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> done(small) == 10);

        assertThat(done(big)).as("the small tenant only finished once almost all of the flood had").isGreaterThan(180);
    }

    long done(Tenant tenant) {
        return mongo.count(Query.query(Criteria.where("tenantId").is(tenant.id()).and("status").is(JobStatus.SUCCEEDED)), Job.class);
    }
}
