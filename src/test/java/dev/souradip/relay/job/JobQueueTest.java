package dev.souradip.relay.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.tenant.CreatedTenant;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;

class JobQueueTest extends IntegrationTestBase {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Autowired
    JobRepository jobs;

    @Autowired
    JobService jobService;

    @Test
    void acceptsWith202AndFinishesInTheBackground() throws Exception {
        CreatedTenant team = tenant("docs");

        HttpResponse<String> accepted = submit(team, "{\"prompt\":\"summarise this\",\"maxTokens\":100}", null);

        assertThat(accepted.statusCode()).isEqualTo(202);
        String location = accepted.headers().firstValue("Location").orElseThrow();
        String id = location.substring(location.lastIndexOf('/') + 1);
        Job done = awaitStatus(id, JobStatus.SUCCEEDED);
        assertThat(done.result().text()).startsWith("tok0");
        assertThat(done.result().provider()).isEqualTo("alpha");
        assertThat(done.attempts()).isEqualTo(1);

        assertThat(get(team, location).statusCode()).isEqualTo(200);
        assertThat(get(tenant("someone-else"), location).statusCode()).as("other tenants cannot see it").isEqualTo(404);
    }

    @Test
    void sameIdempotencyKeyReturnsTheSameJob() throws Exception {
        CreatedTenant team = tenant("docs");
        String body = "{\"prompt\":\"summarise this\",\"maxTokens\":100}";

        HttpResponse<String> first = submit(team, body, "doc-42");
        HttpResponse<String> retry = submit(team, body, "doc-42");
        HttpResponse<String> different = submit(team, "{\"prompt\":\"something else\",\"maxTokens\":100}", "doc-42");

        assertThat(retry.headers().firstValue("Location")).isEqualTo(first.headers().firstValue("Location"));
        assertThat(retry.headers().firstValue("Idempotent-Replayed")).hasValue("true");
        assertThat(different.statusCode()).isEqualTo(422);
        assertThat(mongo.count(new Query(), Job.class)).isEqualTo(1);
    }

    @Test
    void outageBecomesADelayNotAnError() {
        CreatedTenant team = tenant("docs");
        ALPHA.down(true);
        BETA.down(true);

        String id = jobService.submit(tenants.find(team.id()).orElseThrow(), new JobRequest("summarise", 50, null), null, "t").job().id();
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(5)).until(() -> jobs.find(id).orElseThrow().attempts() >= 2);
        ALPHA.down(false);

        Job done = awaitStatus(id, JobStatus.SUCCEEDED);
        assertThat(done.attempts()).isBetween(2, 4);
    }

    @Test
    void poisonJobGoesToTheDeadLetterStateWithTheReason() {
        CreatedTenant team = tenant("docs");

        String id = jobService.submit(tenants.find(team.id()).orElseThrow(), new JobRequest("[[poison]]", 50, null), null, "t").job().id();

        Job dead = awaitStatus(id, JobStatus.DEAD);
        assertThat(dead.attempts()).isEqualTo(4);
        assertThat(dead.lastError()).contains("gave up after 4 attempts");
        assertThat(ALPHA.requests() + BETA.requests()).as("each attempt tried both providers").isEqualTo(8);
    }

    @Test
    void rejectedRequestIsNotRetried() {
        CreatedTenant team = tenant("docs");

        String id = jobService.submit(tenants.find(team.id()).orElseThrow(), new JobRequest("[[reject]]", 50, null), null, "t").job().id();

        Job dead = awaitStatus(id, JobStatus.DEAD);
        assertThat(dead.attempts()).isEqualTo(1);
        assertThat(dead.lastError()).contains("rejected");
        assertThat(BETA.requests()).as("a request one provider rejects is not sent to the next").isZero();
    }

    /**
     * A worker claims a job and dies. Its lease runs out, another worker takes the job over and
     * finishes it. Then the first worker turns out to have been only slow and tries to write its
     * result. The lease check refuses it, so the tenant sees one result.
     */
    @Test
    void crashedWorkersJobIsTakenOverAndTheLateResultIsRefused() {
        CreatedTenant team = tenant("docs");
        Instant now = Instant.now();
        String id = UUID.randomUUID().toString();
        mongo.insert(new Job(id, team.id(), null, JobStatus.RUNNING, "summarise", 50, 1, 4, now, "worker-that-died",
                now.plusMillis(500), null, null, null, "t", now, now, null));

        Job done = awaitStatus(id, JobStatus.SUCCEEDED);

        assertThat(done.attempts()).isEqualTo(2);
        assertThat(jobs.succeed(id, "worker-that-died", new JobResult("stale", "alpha", 1, 1), Instant.now())).isFalse();
        assertThat(jobs.find(id).orElseThrow().result().text()).isNotEqualTo("stale");
    }

    private Job awaitStatus(String id, JobStatus status) {
        return await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50))
                .until(() -> jobs.find(id).orElseThrow(), j -> j.status() == status);
    }

    private HttpResponse<String> submit(CreatedTenant team, String body, String idempotencyKey) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl() + "/v1/jobs"))
                .header("Content-Type", "application/json")
                .header("X-Api-Key", team.apiKey())
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(CreatedTenant team, String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(baseUrl() + path)).header("X-Api-Key", team.apiKey()).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
