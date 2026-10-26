package dev.souradip.relay.provider;

import java.util.List;

/** @param requestId sent upstream so a provider that supports it can dedupe retries */
public record ModelRequest(List<Message> messages, int maxTokens, String requestId) {}
