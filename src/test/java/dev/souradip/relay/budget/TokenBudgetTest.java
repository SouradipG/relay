package dev.souradip.relay.budget;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import dev.souradip.relay.IntegrationTestBase;
import dev.souradip.relay.api.ApiException;
import dev.souradip.relay.tenant.Tenant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class TokenBudgetTest extends IntegrationTestBase {

    @Autowired
    TokenBudget budget;

    @Test
    void reservesUntilEmptyThenSaysWhenToComeBack() {
        Tenant team = tenants.find(tenant("t", 1, 6000, 1).id()).orElseThrow();

        assertThat(budget.tryReserve(team, 5000)).isInstanceOf(TokenBudget.Decision.Granted.class);
        TokenBudget.Decision second = budget.tryReserve(team, 5000);

        assertThat(second).isInstanceOfSatisfying(TokenBudget.Decision.Denied.class,
                d -> assertThat(d.retryAfterMillis()).as("4,000 short at 100 tokens a second").isBetween(35_000L, 41_000L));
    }

    @Test
    void settlingRefundsAnOverestimateAndChargesAnUnderestimate() {
        Tenant team = tenants.find(tenant("t", 1, 6000, 1).id()).orElseThrow();
        TokenBudget.Reservation big = ((TokenBudget.Decision.Granted) budget.tryReserve(team, 5000)).reservation();

        budget.settle(big, 200);
        assertThat(budget.remaining(team)).isBetween(5800.0, 5900.0);

        TokenBudget.Reservation small = ((TokenBudget.Decision.Granted) budget.tryReserve(team, 100)).reservation();
        budget.settle(small, 5900);
        assertThat(budget.remaining(team)).as("the tenant is now in debt and must wait").isNegative();
        assertThat(budget.tryReserve(team, 1)).isInstanceOf(TokenBudget.Decision.Denied.class);
    }

    @Test
    void concurrentReservationsNeverOverspend() throws Exception {
        Tenant team = tenants.find(tenant("t", 1, 10_000, 1).id()).orElseThrow();
        List<Future<TokenBudget.Decision>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 300; i++) {
                results.add(pool.submit(() -> budget.tryReserve(team, 100)));
            }
        }
        long granted = 0;
        for (Future<TokenBudget.Decision> result : results) {
            if (result.get() instanceof TokenBudget.Decision.Granted) {
                granted++;
            }
        }
        // 100 from the bucket, plus whatever refilled while the test ran (about 1.7 tokens per 10 ms).
        assertThat(granted).isBetween(100L, 103L);
    }

    @Test
    void requestLargerThanTheWholeBudgetIsRefused() {
        Tenant team = tenants.find(tenant("t", 1, 1000, 1).id()).orElseThrow();

        assertThat(catchThrowable(() -> budget.tryReserve(team, 1001)))
                .isInstanceOfSatisfying(ApiException.class, e -> assertThat(e.code()).isEqualTo("REQUEST_TOO_LARGE"));
    }
}
