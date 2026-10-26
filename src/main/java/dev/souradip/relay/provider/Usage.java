package dev.souradip.relay.provider;

public record Usage(long inputTokens, long outputTokens) {

    public long total() {
        return inputTokens + outputTokens;
    }
}
