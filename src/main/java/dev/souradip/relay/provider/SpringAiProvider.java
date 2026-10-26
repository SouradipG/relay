package dev.souradip.relay.provider;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import reactor.core.Disposable;

/**
 * Routes to any model Spring AI supports (Ollama, Bedrock, Vertex, Anthropic, OpenAI, ...) by
 * adding that model's Spring AI starter. The gateway's budgets, breaker and limits apply the
 * same way as for the HTTP providers.
 */
public class SpringAiProvider implements ModelProvider {

    private static final Object END = new Object();

    private final String name;
    private final ChatModel model;

    public SpringAiProvider(String name, ChatModel model) {
        this.name = name;
        this.model = model;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Completion complete(ModelRequest request) throws ProviderException {
        try {
            ChatResponse response = model.call(prompt(request));
            return new Completion(response.getResult().getOutput().getText(), usageOf(response));
        } catch (RuntimeException e) {
            throw new ProviderException(ProviderException.Kind.UNAVAILABLE, name, e.toString(), 0, e);
        }
    }

    @Override
    public TokenStream stream(ModelRequest request) {
        BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
        Disposable subscription = model.stream(prompt(request)).subscribe(queue::add, queue::add, () -> queue.add(END));
        return new TokenStream() {
            private dev.souradip.relay.provider.Usage usage;
            private int received;

            @Override
            public String next() throws ProviderException {
                try {
                    while (true) {
                        Object item = queue.take();
                        if (item == END) {
                            return null;
                        }
                        if (item instanceof Throwable error) {
                            throw new ProviderException(ProviderException.Kind.BROKEN, name, error.toString(), 0, error);
                        }
                        ChatResponse response = (ChatResponse) item;
                        dev.souradip.relay.provider.Usage reported = usageOf(response);
                        if (reported != null && reported.total() > 0) {
                            usage = reported;
                        }
                        String text = response.getResult() == null ? null : response.getResult().getOutput().getText();
                        if (text != null && !text.isEmpty()) {
                            received++;
                            return text;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ProviderException(ProviderException.Kind.BROKEN, name, "interrupted", 0, e);
                }
            }

            @Override
            public dev.souradip.relay.provider.Usage usage() {
                return usage;
            }

            @Override
            public int received() {
                return received;
            }

            @Override
            public void close() {
                subscription.dispose();
            }
        };
    }

    private static Prompt prompt(ModelRequest request) {
        List<org.springframework.ai.chat.messages.Message> messages = request.messages().stream()
                .<org.springframework.ai.chat.messages.Message>map(m -> switch (m.role()) {
                    case "system" -> new SystemMessage(m.content());
                    case "assistant" -> new AssistantMessage(m.content());
                    default -> new UserMessage(m.content());
                })
                .toList();
        return new Prompt(messages, ChatOptions.builder().maxTokens(request.maxTokens()).build());
    }

    private static dev.souradip.relay.provider.Usage usageOf(ChatResponse response) {
        if (response.getMetadata() == null || response.getMetadata().getUsage() == null) {
            return null;
        }
        Usage usage = response.getMetadata().getUsage();
        return new dev.souradip.relay.provider.Usage(
                usage.getPromptTokens() == null ? 0 : usage.getPromptTokens(),
                usage.getCompletionTokens() == null ? 0 : usage.getCompletionTokens());
    }
}
