package dev.souradip.relay.job;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record JobRequest(
        @NotBlank @Size(max = 200_000) String prompt,
        @NotNull @Min(1) @Max(8192) Integer maxTokens,
        @Pattern(regexp = "https?://.+") String webhookUrl) {}
