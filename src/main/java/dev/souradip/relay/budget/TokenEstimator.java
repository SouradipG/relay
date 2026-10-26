package dev.souradip.relay.budget;

import dev.souradip.relay.provider.Message;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * A cheap upper bound on what a call will cost, used to reserve budget before the call.
 *
 * <p>Four characters per token is the usual rule of thumb for English with BPE tokenisers, plus
 * a small overhead per message for role markers. The output side is charged at the requested
 * maximum, because the model may use all of it. Settlement refunds what was not used.
 */
@Component
public class TokenEstimator {

    private static final int PER_MESSAGE_OVERHEAD = 4;

    public long inputTokens(List<Message> messages) {
        long total = 0;
        for (Message message : messages) {
            total += (message.content().length() + 3) / 4 + PER_MESSAGE_OVERHEAD;
        }
        return total;
    }

    public long estimate(List<Message> messages, int maxTokens) {
        return inputTokens(messages) + maxTokens;
    }
}
