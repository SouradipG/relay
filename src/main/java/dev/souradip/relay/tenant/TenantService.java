package dev.souradip.relay.tenant;

import dev.souradip.relay.api.ApiException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class TenantService {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Duration CACHE_TTL = Duration.ofSeconds(10);

    private record Cached(Tenant tenant, Instant until) {}

    private final MongoTemplate mongo;
    // Every request authenticates. A short in memory cache keeps that off the database without
    // letting a revoked key live for long.
    private final Map<String, Cached> byKeyHash = new ConcurrentHashMap<>();

    TenantService(MongoTemplate mongo) {
        this.mongo = mongo;
        mongo.indexOps(Tenant.class).createIndex(new Index().on("apiKeyHash", Sort.Direction.ASC).unique());
    }

    public CreatedTenant create(CreateTenantRequest request) {
        String apiKey = "rk_" + randomToken();
        String webhookSecret = "whsec_" + randomToken();
        Tenant tenant = new Tenant(UUID.randomUUID().toString(), request.name(), hash(apiKey), webhookSecret,
                request.weight(), request.tokensPerMinute(), request.maxConcurrentJobs(), Instant.now());
        mongo.insert(tenant);
        return new CreatedTenant(tenant.id(), tenant.name(), apiKey, webhookSecret);
    }

    public Tenant authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw unauthorised();
        }
        String hash = hash(apiKey);
        Cached cached = byKeyHash.get(hash);
        if (cached != null && cached.until().isAfter(Instant.now())) {
            return cached.tenant();
        }
        Tenant tenant = mongo.findOne(Query.query(Criteria.where("apiKeyHash").is(hash)), Tenant.class);
        if (tenant == null) {
            throw unauthorised();
        }
        byKeyHash.put(hash, new Cached(tenant, Instant.now().plus(CACHE_TTL)));
        return tenant;
    }

    public Optional<Tenant> find(String id) {
        return Optional.ofNullable(mongo.findById(id, Tenant.class));
    }

    private static ApiException unauthorised() {
        return new ApiException(HttpStatus.UNAUTHORIZED, "UNAUTHORISED", "a valid X-Api-Key is required");
    }

    private static String randomToken() {
        byte[] bytes = new byte[24];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    static String hash(String apiKey) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(apiKey.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
