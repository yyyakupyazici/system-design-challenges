package io.paramix.notify.router;

import io.paramix.notify.domain.NotificationEvent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Her öncelik için ayrı consumer grubu ve thread havuzu: kampanya router'ı meşgul ederken
 * OTP aynı thread'lerin arkasında beklemez (topic izolasyonunun router katmanındaki karşılığı).
 */
@Component
@ConditionalOnProperty(name = "notify.roles.router", havingValue = "true", matchIfMissing = true)
public class RouterListeners {

    private final RoutingService routing;
    private final ObjectMapper json;

    public RouterListeners(RoutingService routing, ObjectMapper json) {
        this.routing = routing;
        this.json = json;
    }

    @KafkaListener(id = "router-critical", topics = "notif.critical", groupId = "router-critical",
            concurrency = "${notify.router.concurrency.critical:4}")
    public void critical(String payload) throws Exception {
        routing.route(json.readValue(payload, NotificationEvent.class));
    }

    @KafkaListener(id = "router-transactional", topics = "notif.transactional", groupId = "router-transactional",
            concurrency = "${notify.router.concurrency.transactional:4}")
    public void transactional(String payload) throws Exception {
        routing.route(json.readValue(payload, NotificationEvent.class));
    }

    @KafkaListener(id = "router-marketing", topics = "notif.marketing", groupId = "router-marketing",
            concurrency = "${notify.router.concurrency.marketing:2}")
    public void marketing(String payload) throws Exception {
        routing.route(json.readValue(payload, NotificationEvent.class));
    }
}
