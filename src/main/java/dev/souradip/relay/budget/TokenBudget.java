package dev.souradip.relay.budget;

import dev.souradip.relay.api.ApiException;
import dev.souradip.relay.tenant.Tenant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Tokens per minute per tenant, shared by every gateway instance through Redis.
 *
 * <p>The cost of a model call is only known after the call. So the flow is: estimate, reserve
 * the estimate, call, then settle with what the provider reports. Without the settle step a
 * budget drifts by the estimation error on every call, in whichever direction the estimator
 * is wrong.
 */
@Component
public class TokenBudget {

    private static final Logger log = LoggerFactory.getLogger(TokenBudget.class);

    public sealed interface Decision {
        record Granted(Reservation reservation) implements Decision {}

        record Denied(long retryAfterMillis) implements Decision {}
    }

    /** Tokens held for one call. Settle it exactly once. */
    public record Reservation(String tenantId, long capacity, long reserved) {}

    @SuppressWarnings("rawtypes")
    private final RedisScript<List> reserve = RedisScript.of(new ClassPathResource("scripts/budget_reserve.lua"), List.class);
    private final RedisScript<String> settle = RedisScript.of(new ClassPathResource("scripts/budget_settle.lua"), String.class);
    private final StringRedisTemplate redis;

    TokenBudget(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Decision tryReserve(Tenant tenant, long tokens) {
        if (tokens > tenant.tokensPerMinute()) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "REQUEST_TOO_LARGE",
                    "this request needs about " + tokens + " tokens, more than the tenant's whole per minute budget");
        }
        List<?> result = redis.execute(reserve, List.of(key(tenant.id())),
                String.valueOf(tenant.tokensPerMinute()), String.valueOf(tokens));
        if (((Number) result.get(0)).longValue() == 1) {
            return new Decision.Granted(new Reservation(tenant.id(), tenant.tokensPerMinute(), tokens));
        }
        return new Decision.Denied(((Number) result.get(2)).longValue());
    }

    /** Settles a reservation with the real usage: refunds an overestimate, charges an underestimate. */
    public void settle(Reservation reservation, long actualTokens) {
        long delta = reservation.reserved() - actualTokens;
        if (delta < 0) {
            log.info("tenant {} used {} tokens over the estimate of {}", reservation.tenantId(), -delta, reservation.reserved());
        }
        if (delta != 0) {
            redis.execute(settle, List.of(key(reservation.tenantId())),
                    String.valueOf(reservation.capacity()), String.valueOf(delta));
        }
    }

    public double remaining(Tenant tenant) {
        Object tokens = redis.opsForHash().get(key(tenant.id()), "tokens");
        return tokens == null ? tenant.tokensPerMinute() : Double.parseDouble(tokens.toString());
    }

    private static String key(String tenantId) {
        return "relay:budget:{" + tenantId + "}";
    }
}
