package dev.souradip.relay.webhook;

import dev.souradip.relay.job.Job;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

@Service
public class WebhookService {

    private final WebhookRepository deliveries;
    private final JsonMapper json;

    WebhookService(WebhookRepository deliveries, JsonMapper json) {
        this.deliveries = deliveries;
        this.json = json;
    }

    /** Called once a job reaches a final state. Safe to call twice for the same job. */
    public void jobFinished(Job job) {
        if (job.webhookUrl() == null) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("deliveryId", job.id());
        payload.put("event", "job.finished");
        payload.put("jobId", job.id());
        payload.put("status", job.status().name());
        payload.put("result", job.result());
        payload.put("error", job.lastError());
        Instant now = Instant.now();
        deliveries.enqueue(new WebhookDelivery(job.id(), job.tenantId(), job.webhookUrl(), json.writeValueAsString(payload),
                WebhookDelivery.DeliveryStatus.PENDING, 0, now, null, null, null, null, now, null));
    }
}
