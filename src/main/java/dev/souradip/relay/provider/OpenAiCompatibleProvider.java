package dev.souradip.relay.provider;

import dev.souradip.relay.RelayProperties;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Any endpoint that speaks the OpenAI chat completions protocol: OpenAI itself, Azure OpenAI,
 * vLLM, Ollama's compatible API, most gateways, and the simulator used in tests.
 *
 * <p>Three timeouts, because one is not enough for streams. Connect, for a host that is down.
 * First byte, for a provider that accepts the connection and then sits on it. Idle, for a
 * stream that started and then stopped making progress. A plain read timeout would either kill
 * healthy long streams or let a stalled one hold a connection forever.
 */
public class OpenAiCompatibleProvider implements ModelProvider {

    private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "stream-watchdog");
        t.setDaemon(true);
        return t;
    });

    private final RelayProperties.Provider config;
    private final HttpClient http;
    private final JsonMapper json;

    public OpenAiCompatibleProvider(RelayProperties.Provider config, JsonMapper json) {
        this.config = config;
        this.json = json;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(config.connectTimeout())
                .build();
    }

    @Override
    public String name() {
        return config.name();
    }

    @Override
    public Completion complete(ModelRequest request) throws ProviderException {
        HttpResponse<InputStream> response = send(request, false);
        try (InputStream body = response.body()) {
            JsonNode root = json.readTree(body);
            String text = root.path("choices").path(0).path("message").path("content").asString("");
            return new Completion(text, parseUsage(root.path("usage")));
        } catch (IOException | RuntimeException e) {
            throw new ProviderException(ProviderException.Kind.BROKEN, name(), "unreadable response", 0, e);
        }
    }

    @Override
    public TokenStream stream(ModelRequest request) throws ProviderException {
        return new SseTokenStream(send(request, true).body());
    }

    private HttpResponse<InputStream> send(ModelRequest request, boolean stream) throws ProviderException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", config.model());
        body.put("messages", request.messages().stream().map(m -> Map.of("role", m.role(), "content", m.content())).toList());
        body.put("max_tokens", request.maxTokens());
        body.put("stream", stream);
        if (stream) {
            body.put("stream_options", Map.of("include_usage", true));
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.baseUrl() + "/v1/chat/completions"))
                // For a streaming call this bounds the wait for the status line, not the whole stream.
                .timeout(config.firstByteTimeout())
                .header("Content-Type", "application/json")
                .header("X-Request-Id", request.requestId())
                .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
        if (config.apiKey() != null && !config.apiKey().isBlank()) {
            builder.header("Authorization", "Bearer " + config.apiKey());
        }

        HttpResponse<InputStream> response;
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpTimeoutException e) {
            throw new ProviderException(ProviderException.Kind.TIMEOUT, name(), "no response within " + config.firstByteTimeout(), 0, e);
        } catch (ConnectException e) {
            throw new ProviderException(ProviderException.Kind.UNAVAILABLE, name(), "connection refused", 0, e);
        } catch (IOException e) {
            throw new ProviderException(ProviderException.Kind.UNAVAILABLE, name(), e.toString(), 0, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProviderException(ProviderException.Kind.UNAVAILABLE, name(), "interrupted", 0, e);
        }

        int status = response.statusCode();
        if (status == 200) {
            return response;
        }
        closeQuietly(response.body());
        if (status == 429) {
            long retryAfter = response.headers().firstValueAsLong("Retry-After").orElse(1) * 1000;
            throw new ProviderException(ProviderException.Kind.RATE_LIMITED, name(), "429 from provider", retryAfter, null);
        }
        if (status >= 500) {
            throw new ProviderException(ProviderException.Kind.UNAVAILABLE, name(), status + " from provider");
        }
        throw new ProviderException(ProviderException.Kind.REJECTED, name(), status + " from provider");
    }

    private static Usage parseUsage(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        return new Usage(node.path("prompt_tokens").asLong(0), node.path("completion_tokens").asLong(0));
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    /** Reads {@code data: ...} lines until {@code data: [DONE]}. */
    private final class SseTokenStream implements TokenStream {

        private final InputStream body;
        private final BufferedReader reader;
        private final ScheduledFuture<?> watchdog;
        private volatile long lastProgressNanos = System.nanoTime();
        private volatile boolean closed;
        private volatile boolean stalled;
        private Usage usage;
        private int received;
        private boolean done;

        SseTokenStream(InputStream body) {
            this.body = body;
            this.reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
            long idle = config.idleTimeout().toNanos();
            long check = Math.max(Duration.ofMillis(50).toNanos(), idle / 4);
            this.watchdog = WATCHDOG.scheduleAtFixedRate(() -> {
                if (System.nanoTime() - lastProgressNanos > idle) {
                    stalled = true;
                    close();
                }
            }, check, check, TimeUnit.NANOSECONDS);
        }

        @Override
        public String next() throws ProviderException {
            if (done) {
                return null;
            }
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    lastProgressNanos = System.nanoTime();
                    if (!line.startsWith("data:")) {
                        continue;
                    }
                    String data = line.substring(5).trim();
                    if (data.equals("[DONE]")) {
                        finish();
                        return null;
                    }
                    JsonNode chunk = json.readTree(data);
                    Usage reported = parseUsage(chunk.path("usage"));
                    if (reported != null) {
                        usage = reported;
                    }
                    List<JsonNode> choices = chunk.path("choices").valueStream().toList();
                    if (!choices.isEmpty()) {
                        String text = choices.getFirst().path("delta").path("content").asString("");
                        if (!text.isEmpty()) {
                            received++;
                            return text;
                        }
                    }
                }
            } catch (IOException | RuntimeException e) {
                throw broken(stalled ? "no data for " + config.idleTimeout() : "stream failed: " + e, e);
            }
            throw broken(stalled ? "no data for " + config.idleTimeout() : "stream ended without [DONE]", null);
        }

        private ProviderException broken(String message, Throwable cause) {
            close();
            return new ProviderException(ProviderException.Kind.BROKEN, name(), message + " after " + received + " chunks", 0, cause);
        }

        private void finish() {
            done = true;
            close();
        }

        @Override
        public Usage usage() {
            return usage;
        }

        @Override
        public int received() {
            return received;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            watchdog.cancel(false);
            // Closing the body closes the connection, which is how the provider learns we are
            // gone and stops generating (and billing) tokens.
            closeQuietly(body);
        }
    }
}
