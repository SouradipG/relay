package dev.souradip.relay.webhook;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.job.Job;
import dev.souradip.relay.job.JobRepository;
import dev.souradip.relay.job.JobRequest;
import dev.souradip.relay.job.JobService;
import dev.souradip.relay.tenant.CreatedTenant;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class WebhookTest extends IntegrationTestBase {

    @Autowired
    JobService jobService;

    @Autowired
    JobRepository jobs;

    @Autowired
    WebhookRepository deliveries;

    @Autowired
    WebhookService webhooks;

    private HttpServer receiver;
    private final List<String> verifiedBodies = new CopyOnWriteArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();
    private volatile int answer = 200;
    private volatile String secret;

    @BeforeEach
    void startReceiver() throws Exception {
        receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/hook", exchange -> {
            calls.incrementAndGet();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String signature = exchange.getRequestHeaders().getFirst("Relay-Signature");
            if (answer == 200 && WebhookSigner.verify(secret, signature, body, Instant.now().getEpochSecond(), 300)) {
                verifiedBodies.add(body);
            }
            exchange.sendResponseHeaders(answer, -1);
            exchange.close();
        });
        receiver.start();
    }

    @AfterEach
    void stopReceiver() {
        receiver.stop(0);
    }

    @Test
    void finishedJobIsDeliveredOnceAndSigned() {
        CreatedTenant team = tenant("docs");
        secret = team.webhookSecret();

        Job job = submitWithHook(team);

        await().atMost(Duration.ofSeconds(10)).until(() -> verifiedBodies.size() == 1);
        assertThat(verifiedBodies.getFirst()).contains(job.id()).contains("SUCCEEDED");
        assertThat(deliveries.find(job.id()).orElseThrow().status()).isEqualTo(WebhookDelivery.DeliveryStatus.DELIVERED);
    }

    @Test
    void aJobThatRanTwiceStillProducesOneDelivery() {
        CreatedTenant team = tenant("docs");
        secret = team.webhookSecret();
        Job job = submitWithHook(team);
        await().atMost(Duration.ofSeconds(10)).until(() -> verifiedBodies.size() == 1);

        webhooks.jobFinished(jobs.find(job.id()).orElseThrow());

        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2)).until(() -> verifiedBodies.size() == 1);
    }

    @Test
    void receiverDownRetriesThenParksAndCanBeReplayed() {
        CreatedTenant team = tenant("docs");
        secret = team.webhookSecret();
        answer = 503;

        Job job = submitWithHook(team);

        await().atMost(Duration.ofSeconds(15)).until(() ->
                deliveries.find(job.id()).map(d -> d.status() == WebhookDelivery.DeliveryStatus.PARKED).orElse(false));
        assertThat(calls.get()).as("max-attempts is 4").isEqualTo(4);
        assertThat(deliveries.find(job.id()).orElseThrow().lastStatusCode()).isEqualTo(503);

        answer = 200;
        assertThat(deliveries.replay(job.id(), team.id(), Instant.now())).isTrue();

        await().atMost(Duration.ofSeconds(10)).until(() -> verifiedBodies.size() == 1);
        assertThat(deliveries.find(job.id()).orElseThrow().status()).isEqualTo(WebhookDelivery.DeliveryStatus.DELIVERED);
    }

    @Test
    void signatureRejectsTamperingAndOldReplays() {
        long now = Instant.now().getEpochSecond();
        String header = WebhookSigner.sign("whsec_test", now, "{\"a\":1}");

        assertThat(WebhookSigner.verify("whsec_test", header, "{\"a\":1}", now, 300)).isTrue();
        assertThat(WebhookSigner.verify("whsec_test", header, "{\"a\":2}", now, 300)).isFalse();
        assertThat(WebhookSigner.verify("whsec_other", header, "{\"a\":1}", now, 300)).isFalse();
        assertThat(WebhookSigner.verify("whsec_test", header, "{\"a\":1}", now + 3600, 300))
                .as("a captured request replayed an hour later").isFalse();
        assertThat(WebhookSigner.verify("whsec_test", null, "{\"a\":1}", now, 300)).isFalse();
    }

    private Job submitWithHook(CreatedTenant team) {
        String url = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/hook";
        return jobService.submit(tenants.find(team.id()).orElseThrow(), new JobRequest("summarise", 20, url), null, "t").job();
    }
}
