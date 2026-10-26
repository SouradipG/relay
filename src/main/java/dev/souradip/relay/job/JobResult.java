package dev.souradip.relay.job;

public record JobResult(String text, String provider, long inputTokens, long outputTokens) {}
