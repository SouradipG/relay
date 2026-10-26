package dev.souradip.relay.chat;

import static dev.souradip.relay.SseClient.chatBody;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.SseClient;
import dev.souradip.relay.tenant.CreatedTenant;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** What a rolling deploy does to open streams, driven through the same lifecycle hook Spring calls on shutdown. */
class DrainTest extends IntegrationTestBase {

    @Autowired
    StreamRegistry registry;

    @AfterEach
    void reopen() {
        registry.start();
    }

    @Test
    void streamsThatFinishWithinTheDrainWindowAreNotCut() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.responseTokens(40).tokenDelay(Duration.ofMillis(20));

        CompletableFuture<SseClient.Result> inFlight = CompletableFuture.supplyAsync(() -> chat(team, 100));
        await().until(() -> registry.openStreams() == 1);

        CompletableFuture<Void> stopping = CompletableFuture.runAsync(registry::stop);
        await().until(registry::draining);
        SseClient.Result refused = SseClient.chat(baseUrl(), team.apiKey(), chatBody("late", 10));
        stopping.join();

        assertThat(refused.status()).as("new streams go to another instance").isEqualTo(503);
        assertThat(inFlight.join().named("done")).as("the open stream finished normally").hasSize(1);
        assertThat(inFlight.join().named("token")).hasSize(40);
    }

    @Test
    void streamsStillOpenAtTheDeadlineAreToldToRetry() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.responseTokens(400).tokenDelay(Duration.ofMillis(20));

        CompletableFuture<SseClient.Result> inFlight = CompletableFuture.supplyAsync(() -> chat(team, 1000));
        await().until(() -> registry.openStreams() == 1);
        registry.stop();

        SseClient.Result result = inFlight.join();
        assertThat(result.named("error")).singleElement().satisfies(e -> assertThat(e.data()).contains("SERVER_RESTARTING"));
        await().atMost(Duration.ofSeconds(5)).until(() -> ALPHA.cancelled() == 1);
    }

    private SseClient.Result chat(CreatedTenant team, int maxTokens) {
        try {
            return SseClient.chat(baseUrl(), team.apiKey(), chatBody("long answer", maxTokens));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
