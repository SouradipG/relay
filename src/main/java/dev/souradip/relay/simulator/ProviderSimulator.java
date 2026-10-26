package dev.souradip.relay.simulator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * A model provider that misbehaves on demand, speaking the OpenAI chat completions protocol.
 *
 * <p>A stub that always answers instantly and politely never exercises retry, failover or
 * cancellation code, so that code first runs for real in production. This one can be slow,
 * return 429 for a share of calls, go down, cut a stream partway, stall mid stream, and it
 * counts what it would have billed, including tokens generated after a client hung up.
 *
 * <p>Prompts can also ask for trouble: {@code [[poison]]} always fails with 500, and
 * {@code [[reject]]} always gets a 400.
 */
public class ProviderSimulator {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private HttpServer server;

    private volatile int responseTokens = 12;
    private volatile Duration tokenDelay = Duration.ofMillis(5);
    private volatile Duration firstTokenDelay = Duration.ZERO;
    private volatile double rateLimitedShare;
    private volatile boolean down;
    private volatile int breakAfterTokens = -1;
    private volatile int stallAfterTokens = -1;
    private final AtomicInteger failNext = new AtomicInteger();
    private volatile String lastPrompt;

    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger rateLimited = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();
    private final AtomicLong tokensGenerated = new AtomicLong();

    public ProviderSimulator start(int port) {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 512);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/v1/chat/completions", this::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        return this;
    }

    public void stop() {
        server.stop(0);
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public void reset() {
        responseTokens = 12;
        tokenDelay = Duration.ofMillis(5);
        firstTokenDelay = Duration.ZERO;
        rateLimitedShare = 0;
        down = false;
        breakAfterTokens = -1;
        stallAfterTokens = -1;
        failNext.set(0);
        lastPrompt = null;
        requests.set(0);
        rateLimited.set(0);
        completed.set(0);
        cancelled.set(0);
        maxInFlight.set(0);
        tokensGenerated.set(0);
    }

    public ProviderSimulator responseTokens(int n) { this.responseTokens = n; return this; }
    public ProviderSimulator tokenDelay(Duration d) { this.tokenDelay = d; return this; }
    public ProviderSimulator firstTokenDelay(Duration d) { this.firstTokenDelay = d; return this; }
    public ProviderSimulator rateLimitedShare(double share) { this.rateLimitedShare = share; return this; }
    public ProviderSimulator down(boolean down) { this.down = down; return this; }
    public ProviderSimulator breakAfterTokens(int n) { this.breakAfterTokens = n; return this; }
    public ProviderSimulator stallAfterTokens(int n) { this.stallAfterTokens = n; return this; }
    public ProviderSimulator failNext(int n) { this.failNext.set(n); return this; }

    public int requests() { return requests.get(); }
    public int rateLimited() { return rateLimited.get(); }
    public int completed() { return completed.get(); }
    public int cancelled() { return cancelled.get(); }
    public int maxInFlight() { return maxInFlight.get(); }
    public long tokensGenerated() { return tokensGenerated.get(); }
    public String lastPrompt() { return lastPrompt; }

    private void handle(HttpExchange exchange) throws IOException {
        requests.incrementAndGet();
        int now = inFlight.incrementAndGet();
        maxInFlight.accumulateAndGet(now, Math::max);
        try (exchange) {
            JsonNode body = JSON.readTree(exchange.getRequestBody());
            String prompt = body.path("messages").valueStream().map(m -> m.path("content").asString("")).reduce("", String::concat);
            lastPrompt = prompt;

            if (down || failNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0 || prompt.contains("[[poison]]")) {
                respond(exchange, 503, "{\"error\":\"unavailable\"}");
                return;
            }
            if (prompt.contains("[[reject]]")) {
                respond(exchange, 400, "{\"error\":\"invalid request\"}");
                return;
            }
            if (rateLimitedShare > 0 && ThreadLocalRandom.current().nextDouble() < rateLimitedShare) {
                rateLimited.incrementAndGet();
                exchange.getResponseHeaders().add("Retry-After", "1");
                respond(exchange, 429, "{\"error\":\"rate limited\"}");
                return;
            }

            int maxTokens = body.path("max_tokens").asInt(256);
            int tokens = Math.min(maxTokens, responseTokens);
            long promptTokens = (prompt.length() + 3) / 4;
            if (body.path("stream").asBoolean(false)) {
                stream(exchange, tokens, promptTokens);
            } else {
                sleep(firstTokenDelay);
                for (int i = 0; i < tokens; i++) {
                    sleep(tokenDelay);
                }
                tokensGenerated.addAndGet(tokens);
                completed.incrementAndGet();
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < tokens; i++) {
                    text.append("tok").append(i).append(' ');
                }
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("choices", List.of(Map.of("message", Map.of("role", "assistant", "content", text.toString().trim()))));
                response.put("usage", Map.of("prompt_tokens", promptTokens, "completion_tokens", tokens));
                respond(exchange, 200, JSON.writeValueAsString(response));
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private void stream(HttpExchange exchange, int tokens, long promptTokens) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
        exchange.sendResponseHeaders(200, 0);
        OutputStream out = exchange.getResponseBody();
        try {
            // Real providers answer with headers at once and then take a while for the first token.
            out.flush();
            sleep(firstTokenDelay);
            for (int i = 0; i < tokens; i++) {
                if (i == breakAfterTokens) {
                    // Ends the response without [DONE], as a provider crash or a dropped connection would.
                    return;
                }
                if (i == stallAfterTokens) {
                    sleep(Duration.ofMinutes(5));
                }
                sleep(tokenDelay);
                tokensGenerated.incrementAndGet();
                String chunk = JSON.writeValueAsString(Map.of("choices", List.of(Map.of("delta", Map.of("content", "tok" + i + " ")))));
                out.write(("data: " + chunk + "\n\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
            String usage = JSON.writeValueAsString(Map.of("choices", List.of(),
                    "usage", Map.of("prompt_tokens", promptTokens, "completion_tokens", tokens)));
            out.write(("data: " + usage + "\n\ndata: [DONE]\n\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            completed.incrementAndGet();
        } catch (IOException clientWentAway) {
            // The gateway closed the connection. A real provider stops generating here.
            cancelled.incrementAndGet();
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static void sleep(Duration duration) {
        if (duration.isZero()) {
            return;
        }
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
