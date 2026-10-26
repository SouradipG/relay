package dev.souradip.relay.tenant;

/** Returned once, at creation. Only a hash of the key is stored, so it cannot be shown again. */
public record CreatedTenant(String id, String name, String apiKey, String webhookSecret) {}
