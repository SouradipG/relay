package dev.souradip.relay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** A minimal Server Sent Events client over a real socket, so tests see what a browser would. */
public final class SseClient {

    public record Event(String name, String data) {}

    public record Result(int status, String errorBody, List<Event> events, int comments, String retryAfter) {

        public List<Event> named(String name) {
            return events.stream().filter(e -> e.name().equals(name)).toList();
        }
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    private SseClient() {}

    public static Result chat(String baseUrl, String apiKey, String body) throws Exception {
        return chat(baseUrl, apiKey, body, Integer.MAX_VALUE);
    }

    /** Reads until the stream ends, or hangs up after {@code stopAfterTokens} token events. */
    public static Result chat(String baseUrl, String apiKey, String body, int stopAfterTokens) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/chat"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .header("Accept", "text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (apiKey != null) {
            request.header("X-Api-Key", apiKey);
        }
        HttpResponse<InputStream> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
        String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
        if (response.statusCode() != 200) {
            try (InputStream in = response.body()) {
                return new Result(response.statusCode(), new String(in.readAllBytes(), StandardCharsets.UTF_8), List.of(), 0, retryAfter);
            }
        }

        List<Event> events = new ArrayList<>();
        int comments = 0;
        int tokens = 0;
        InputStream in = response.body();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String name = "message";
            StringBuilder data = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(":")) {
                    comments++;
                } else if (line.startsWith("event:")) {
                    name = line.substring(6).trim();
                } else if (line.startsWith("data:")) {
                    data.append(line.substring(5));
                } else if (line.isEmpty() && !data.isEmpty()) {
                    events.add(new Event(name, data.toString()));
                    if (name.equals("token") && ++tokens >= stopAfterTokens) {
                        break;
                    }
                    name = "message";
                    data.setLength(0);
                }
            }
        } catch (IOException ignored) {
            // the server closed the stream
        }
        return new Result(200, null, events, comments, retryAfter);
    }

    public static String chatBody(String prompt, int maxTokens) {
        return "{\"messages\":[{\"role\":\"user\",\"content\":\"" + prompt + "\"}],\"maxTokens\":" + maxTokens + "}";
    }
}
