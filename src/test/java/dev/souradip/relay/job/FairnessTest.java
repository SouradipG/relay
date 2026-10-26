package dev.souradip.relay.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.tenant.Tenant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 * One team floods the queue with 200 jobs, then another team submits 10. With fair scheduling
 * the small team's jobs run almost straight away instead of waiting behind the whole flood.
 */
class FairnessTest extends IntegrationTestBase {

    @Autowired
    JobService jobService;

    @Test
    void smallTenantIsNotStuckBehindAFlood() {
        Tenant big = tenants.find(tenant("batch", 1, 10_000_000, 4).id()).orElseThrow();
        Tenant small = tenants.find(tenant("interactive", 1, 10_000_000, 4).id()).orElseThrow();
        ALPHA.tokenDelay(Duration.ofMillis(2));

        for (int i = 0; i < 200; i++) {
            jobService.submit(big, new JobRequest("bulk " + i, 20, null), null, "t");
        }
        long bigDoneBefore = done(big);
        List<String> smallJobs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            smallJobs.add(jobService.submit(small, new JobRequest("urgent " + i, 20, null), null, "t").job().id());
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> done(small) == 10);
        long bigRanMeanwhile = done(big) - bigDoneBefore;

        assertThat(bigRanMeanwhile)
                .as("equal weights: the two tenants take turns, so about 10 of the flood ran while the small tenant's 10 did")
                .isLessThan(40);
    }

    long done(Tenant tenant) {
        return mongo.count(Query.query(Criteria.where("tenantId").is(tenant.id()).and("status").is(JobStatus.SUCCEEDED)), Job.class);
    }
}
