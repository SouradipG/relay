package dev.souradip.relay.tenant;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateTenantRequest(
        @NotBlank String name,
        @NotNull @Min(1) @Max(100) Integer weight,
        @NotNull @Min(100) Long tokensPerMinute,
        @NotNull @Min(1) @Max(1000) Integer maxConcurrentJobs) {}
