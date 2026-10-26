package dev.souradip.relay.chat;

import dev.souradip.relay.provider.Message;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record ChatRequest(@NotEmpty List<@Valid Message> messages, @NotNull @Min(1) @Max(8192) Integer maxTokens) {}
