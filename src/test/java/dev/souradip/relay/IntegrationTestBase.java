package dev.souradip.relay;

import dev.souradip.relay.job.Job;
import dev.souradip.relay.routing.Router;
import dev.souradip.relay.simulator.ProviderSimulator;
import dev.souradip.relay.tenant.CreateTenantRequest;
import dev.souradip.relay.tenant.CreatedTenant;
import dev.souradip.relay.tenant.Tenant;
import dev.souradip.relay.tenant.TenantService;
import dev.souradip.relay.webhook.WebhookDelivery;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * Real MongoDB, real Redis, and two simulated providers on real sockets. Timeouts, leases and
 * backoff are shortened so failure scenarios run in seconds.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "relay.admin-token=test-admin",
        "relay.breaker.window=10",
        "relay.breaker.minimum-calls=5",
        "relay.breaker.failure-rate=0.5",
        "relay.breaker.open-for=1s",
        "relay.chat.heartbeat=200ms",
        "relay.chat.drain-timeout=2s",
        "relay.jobs.workers=4",
        "relay.jobs.lease=2s",
        "relay.jobs.max-attempts=4",
        "relay.jobs.retry-base=50ms",
        "relay.jobs.retry-max=200ms",
        "relay.jobs.idle-poll=20ms",
        "relay.webhooks.poll=50ms",
        "relay.webhooks.timeout=1s",
        "relay.webhooks.max-attempts=4",
        "relay.webhooks.retry-base=50ms",
        "relay.webhooks.retry-max=200ms",
        "spring.data.redis.timeout=5s"
})
public abstract class IntegrationTestBase {

    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0");

    @ServiceConnection(name = "redis")
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8-alpine").withExposedPorts(6379);

    protected static final ProviderSimulator ALPHA = new ProviderSimulator().start(0);
    protected static final ProviderSimulator BETA = new ProviderSimulator().start(0);

    static {
        MONGO.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void providers(DynamicPropertyRegistry registry) {
        // A list set here replaces the whole list from application.yml, so every field is given.
        provider(registry, 0, "alpha", ALPHA);
        provider(registry, 1, "beta", BETA);
    }

    private static void provider(DynamicPropertyRegistry registry, int index, String name, ProviderSimulator simulator) {
        String prefix = "relay.providers[" + index + "].";
        registry.add(prefix + "name", () -> name);
        registry.add(prefix + "base-url", simulator::baseUrl);
        registry.add(prefix + "model", () -> "sim");
        registry.add(prefix + "initial-concurrency", () -> 16);
        registry.add(prefix + "max-concurrency", () -> 64);
        registry.add(prefix + "connect-timeout", () -> "500ms");
        registry.add(prefix + "first-byte-timeout", () -> "2s");
        registry.add(prefix + "idle-timeout", () -> "1s");
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected MongoTemplate mongo;

    @Autowired
    protected StringRedisTemplate redis;

    @Autowired
    protected TenantService tenants;

    @Autowired
    protected Router router;

    @BeforeEach
    void cleanState() {
        ALPHA.reset();
        BETA.reset();
        router.resetHealth();
        // Remove documents rather than dropping collections, so the indexes stay.
        mongo.remove(new Query(), Job.class);
        mongo.remove(new Query(), WebhookDelivery.class);
        mongo.remove(new Query(), Tenant.class);
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    protected CreatedTenant tenant(String name, int weight, long tokensPerMinute, int maxConcurrentJobs) {
        return tenants.create(new CreateTenantRequest(name, weight, tokensPerMinute, maxConcurrentJobs));
    }

    protected CreatedTenant tenant(String name) {
        return tenant(name, 1, 1_000_000, 8);
    }

    protected String baseUrl() {
        return "http://localhost:" + port;
    }
}
