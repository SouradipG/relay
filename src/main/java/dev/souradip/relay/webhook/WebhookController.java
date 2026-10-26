package dev.souradip.relay.webhook;

import dev.souradip.relay.api.ApiException;
import dev.souradip.relay.tenant.Tenant;
import dev.souradip.relay.tenant.TenantService;
import java.time.Instant;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
class WebhookController {

    private final WebhookRepository deliveries;
    private final TenantService tenants;

    WebhookController(WebhookRepository deliveries, TenantService tenants) {
        this.deliveries = deliveries;
        this.tenants = tenants;
    }

    @GetMapping("/v1/webhooks/{jobId}")
    Map<String, Object> status(@RequestHeader(value = "X-Api-Key", required = false) String apiKey, @PathVariable String jobId) {
        Tenant tenant = tenants.authenticate(apiKey);
        WebhookDelivery delivery = deliveries.find(jobId).filter(d -> d.tenantId().equals(tenant.id()))
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", "no webhook for this job"));
        return Map.of("status", delivery.status(), "attempts", delivery.attempts(),
                "lastError", delivery.lastError() == null ? "" : delivery.lastError());
    }

    @PostMapping("/v1/webhooks/{jobId}/replay")
    ResponseEntity<Void> replay(@RequestHeader(value = "X-Api-Key", required = false) String apiKey, @PathVariable String jobId) {
        Tenant tenant = tenants.authenticate(apiKey);
        if (!deliveries.replay(jobId, tenant.id(), Instant.now())) {
            throw new ApiException(HttpStatus.CONFLICT, "NOT_PARKED", "only parked deliveries can be replayed");
        }
        return ResponseEntity.accepted().build();
    }
}
