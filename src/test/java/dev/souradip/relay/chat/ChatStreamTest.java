package dev.souradip.relay.chat;

import static dev.souradip.relay.SseClient.chatBody;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.SseClient;
import dev.souradip.relay.budget.TokenBudget;
import dev.souradip.relay.tenant.CreatedTenant;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class ChatStreamTest extends IntegrationTestBase {

    @Autowired
    TokenBudget budget;

    @Test
    void streamsTokensThenUsageAndRefundsTheUnusedEstimate() throws Exception {
        CreatedTenant team = tenant("search", 1, 1000, 4);

        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("hello there", 500));

        assertThat(result.status()).isEqualTo(200);
        assertThat(result.named("token")).hasSize(12);
        assertThat(result.named("token").getFirst().data()).contains("tok0");
        assertThat(result.named("done")).singleElement()
                .satisfies(e -> assertThat(e.data()).contains("\"provider\":\"alpha\"").contains("\"outputTokens\":12"));
        // About 500 tokens were reserved up front. Only about 15 were used, so the rest came back.
        assertThat(budget.remaining(tenants.find(team.id()).orElseThrow())).isGreaterThan(950);
    }

    @Test
    void sendsHeartbeatsWhileTheModelIsThinking() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.firstTokenDelay(Duration.ofMillis(700));

        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("think hard", 50));

        assertThat(result.comments()).as("keep-alive comments every 200 ms stop a load balancer closing an idle stream")
                .isGreaterThanOrEqualTo(2);
        assertThat(result.named("done")).hasSize(1);
    }

    @Test
    void failsOverBeforeTheFirstToken() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.down(true);

        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("hi", 50));

        assertThat(result.named("done")).singleElement().satisfies(e -> assertThat(e.data()).contains("\"provider\":\"beta\""));
    }

    @Test
    void allProvidersRateLimitingIsAFastHonestError() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.rateLimitedShare(1.0);
        BETA.rateLimitedShare(1.0);

        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("hi", 50));

        assertThat(result.status()).isEqualTo(503);
        assertThat(result.errorBody()).contains("NO_PROVIDER").contains("rate limiting");
        assertThat(result.retryAfter()).isNotNull();
    }

    /**
     * The provider dies after five tokens. Quietly retrying on another provider would start the
     * answer again from the top, so the client would see the first five tokens twice and the
     * tenant would pay twice. The client is told how far it got instead.
     */
    @Test
    void brokenStreamIsReportedNotSilentlyRestarted() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.breakAfterTokens(5);

        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("hi", 50));

        assertThat(result.named("token")).hasSize(5);
        assertThat(result.named("error")).singleElement()
                .satisfies(e -> assertThat(e.data()).contains("UPSTREAM_BROKEN").contains("\"delivered\":5"));
        assertThat(BETA.requests()).isZero();
    }

    @Test
    void stalledStreamIsCutByTheIdleTimeout() throws Exception {
        CreatedTenant team = tenant("search");
        ALPHA.stallAfterTokens(3);

        long started = System.nanoTime();
        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("hi", 50));

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
        assertThat(result.named("token")).hasSize(3);
        assertThat(result.named("error")).singleElement().satisfies(e -> assertThat(e.data()).contains("\"delivered\":3"));
    }

    /** The user closes the tab. The provider must stop generating, or we keep paying for tokens nobody reads. */
    @Test
    void clientDisconnectCancelsTheUpstreamCall() throws Exception {
        CreatedTenant team = tenant("search", 1, 10_000, 4);
        ALPHA.responseTokens(400).tokenDelay(Duration.ofMillis(10));

        SseClient.chat(baseUrl(), team.apiKey(), chatBody("write an essay", 2000), 3);

        await().atMost(Duration.ofSeconds(10)).until(() -> ALPHA.cancelled() == 1);
        assertThat(ALPHA.tokensGenerated()).isLessThan(400);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(budget.remaining(tenants.find(team.id()).orElseThrow()))
                        .as("the unused part of the 2,000 token reservation is refunded")
                        .isGreaterThan(9000));
    }

    @Test
    void tenantOverItsBudgetGets429WithRetryAfter() throws Exception {
        CreatedTenant team = tenant("batchy", 1, 1000, 4);
        ALPHA.responseTokens(900).tokenDelay(Duration.ZERO);

        assertThat(SseClient.chat(baseUrl(), team.apiKey(), chatBody("a", 900)).named("done")).hasSize(1);
        SseClient.Result second = SseClient.chat(baseUrl(), team.apiKey(), chatBody("b", 900));

        assertThat(second.status()).isEqualTo(429);
        assertThat(second.errorBody()).contains("TOKEN_BUDGET");
        assertThat(Long.parseLong(second.retryAfter())).isPositive();
    }

    @Test
    void requestBiggerThanTheWholeBudgetIsRefusedOutright() throws Exception {
        CreatedTenant team = tenant("tiny", 1, 100, 1);

        SseClient.Result result = SseClient.chat(baseUrl(), team.apiKey(), chatBody("hi", 500));

        assertThat(result.status()).isEqualTo(422);
        assertThat(result.errorBody()).contains("REQUEST_TOO_LARGE");
    }

    @Test
    void personalDataNeverReachesTheProvider() throws Exception {
        CreatedTenant team = tenant("support");

        SseClient.chat(baseUrl(), team.apiKey(), chatBody("Customer jane.doe@example.com paid with 4111 1111 1111 1111", 20));

        assertThat(ALPHA.lastPrompt()).contains("[EMAIL]").contains("[CARD]")
                .doesNotContain("jane.doe").doesNotContain("4111");
    }

    @Test
    void unknownKeyIsRejected() throws Exception {
        assertThat(SseClient.chat(baseUrl(), "rk_nope", chatBody("hi", 10)).status()).isEqualTo(401);
        assertThat(SseClient.chat(baseUrl(), null, chatBody("hi", 10)).status()).isEqualTo(401);
    }
}
