package dev.souradip.relay.provider;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record Message(@NotBlank @Pattern(regexp = "system|user|assistant") String role, @NotBlank String content) {}
