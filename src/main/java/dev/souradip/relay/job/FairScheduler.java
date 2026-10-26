package dev.souradip.relay.job;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides which tenant's job runs next. Stride scheduling: each tenant has a pass value, the
 * tenant with the lowest pass goes next, and running a job adds {@code 1 / weight} to that
 * tenant's pass. Over time each tenant with work waiting gets worker time in proportion to its
 * weight, however many jobs it has queued.
 *
 * <p>With plain FIFO, a tenant that dumps 50,000 jobs makes a tenant that submits 5 wait behind
 * all of them. Here the small tenant's jobs go next as soon as they arrive.
 *
 * <p>A tenant coming back after being idle starts at the current virtual time, not where it left
 * off, so it cannot bank credit while idle and then monopolise the workers.
 */
class FairScheduler {

    record Candidate(String tenantId, int weight) {}

    private final Map<String, Double> pass = new HashMap<>();
    private double virtualTime;

    /** @return the candidates, best first */
    synchronized List<Candidate> order(List<Candidate> ready) {
        for (Candidate candidate : ready) {
            pass.merge(candidate.tenantId(), virtualTime, Math::max);
        }
        return ready.stream().sorted(Comparator.comparingDouble(c -> pass.get(c.tenantId()))).toList();
    }

    synchronized void charge(Candidate candidate) {
        double current = pass.getOrDefault(candidate.tenantId(), virtualTime);
        virtualTime = Math.max(virtualTime, current);
        pass.put(candidate.tenantId(), current + 1.0 / candidate.weight());
    }
}
