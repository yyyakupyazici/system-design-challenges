package io.paramix.notify.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryMessage;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.provider.FakeProvider;
import io.paramix.notify.provider.FakeProvider.Outcome;
import io.paramix.notify.provider.ProviderRegistry;
import io.paramix.notify.router.UserDirectory;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;
import io.paramix.notify.store.UserRepository;
import io.paramix.notify.worker.WorkerContainers;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Uçtan uca: API → outbox → Kafka → router → teslimat topic'i → worker → (sahte) sağlayıcı.
 * Kafka gömülü; PostgreSQL ve Redis Testcontainers ya da IT_* env değişkenleri.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "notify.kafka.partitions=2",
        "notify.scheduler.outbox-ms=100",
        "notify.scheduler.delayed-ms=200",
        "notify.scheduler.reaper-ms=500",
        "notify.delivery.fallback-wait=3s",
        "notify.delivery.unknown-grace=1s",
        "notify.campaigns.cache-ttl=100ms",
        "notify.router.concurrency.critical=1",
        "notify.router.concurrency.transactional=1",
        "notify.router.concurrency.marketing=1",
        "notify.worker.concurrency.CRITICAL=1",
        "notify.worker.concurrency.TRANSACTIONAL=1",
        "notify.worker.concurrency.MARKETING=1"
})
@EmbeddedKafka(kraft = true, partitions = 2)
@EnabledIf("io.paramix.notify.it.TestInfra#available")
class NotificationFlowIT {

    @DynamicPropertySource
    static void infra(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", TestInfra::pgUrl);
        r.add("spring.datasource.username", TestInfra::pgUser);
        r.add("spring.datasource.password", TestInfra::pgPassword);
        r.add("spring.data.redis.host", TestInfra::redisHost);
        r.add("spring.data.redis.port", TestInfra::redisPort);
    }

    @Autowired TestRestTemplate http;
    @Autowired DeliveryRepository deliveries;
    @Autowired ProviderRegistry providers;
    @Autowired UserRepository users;
    @Autowired UserDirectory directory;
    @Autowired JdbcClient jdbc;
    @Autowired KafkaListenerEndpointRegistry routerContainers;
    @Autowired WorkerContainers workerContainers;

    FakeProvider smsA, smsB, ses, fcm;

    @BeforeEach
    void setUp() {
        smsA = providers.fake("smsA");
        smsB = providers.fake("smsB");
        ses = providers.fake("ses");
        fcm = providers.fake("fcm");
        List.of(smsA, smsB, ses, fcm).forEach(FakeProvider::reset);

        // Testler günün her saatinde çalışabilsin: sessiz saat yok (start == end)
        prefs("u_1", true, true, LocalTime.MIDNIGHT, LocalTime.MIDNIGHT);
        prefs("u_2", false, true, LocalTime.MIDNIGHT, LocalTime.MIDNIGHT);
        prefs("u_3", true, false, LocalTime.MIDNIGHT, LocalTime.MIDNIGHT);

        await().atMost(Duration.ofSeconds(60)).until(() -> workerContainers.allAssigned()
                && routerContainers.getListenerContainers().stream()
                .allMatch(c -> c.getAssignedPartitions() != null && !c.getAssignedPartitions().isEmpty()));
    }

    @AfterEach
    void tearDown() {
        List.of(smsA, smsB, ses, fcm).forEach(FakeProvider::reset);
    }

    // ---------------------------------------------------------------- idempotency

    @Test
    void apiRetry_sameIdempotencyKey_samesNotification_eachChannelSentOnce() {
        Map<String, Object> req = request("TRANSACTIONAL", "u_1", "tx_credited",
                Map.of("sender", "Ayşe K.", "amount", "500,00", "currency", "TRY"), List.of("PUSH", "EMAIL"));

        ResponseEntity<Map> first = http.postForEntity("/notifications", req, Map.class);
        ResponseEntity<Map> retry = http.postForEntity("/notifications", req, Map.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(retry.getBody().get("notificationId")).isEqualTo(first.getBody().get("notificationId"));
        assertThat(retry.getBody().get("duplicate")).isEqualTo(true);

        String id = (String) first.getBody().get("notificationId");
        awaitStatus(id, Channel.PUSH, DeliveryStatus.SENT);
        awaitStatus(id, Channel.EMAIL, DeliveryStatus.SENT);
        assertThat(fcm.handsetCount(id + ":PUSH")).isEqualTo(1);
        assertThat(ses.handsetCount(id + ":EMAIL")).isEqualTo(1);
        assertThat(ses.handsetMessages().getLast().body()).isEqualTo("Merhaba, Ayşe K. sana 500,00 TRY gönderdi.");
    }

    @Test
    void deliveryReport_arrivingThreeTimes_isHarmless_andStatusOnlyMovesForward() {
        String id = send(request("CRITICAL", "u_1", "otp_login", Map.of("code", "482913"), List.of("SMS")));
        awaitStatus(id, Channel.SMS, DeliveryStatus.SENT);
        String msgId = smsA.providerMsgId(id + ":SMS");

        Map<String, Object> dlr = Map.of("providerMsgId", msgId, "status", "DELIVERED");
        assertThat(http.postForObject("/webhooks/smsA/dlr", dlr, Map.class).get("changed")).isEqualTo(true);
        assertThat(http.postForObject("/webhooks/smsA/dlr", dlr, Map.class).get("changed")).isEqualTo(false);
        Map<String, Object> lateFailure = Map.of("providerMsgId", msgId, "status", "FAILED", "error", "late");
        assertThat(http.postForObject("/webhooks/smsA/dlr", lateFailure, Map.class).get("changed")).isEqualTo(false);

        assertThat(delivery(id, Channel.SMS).status()).isEqualTo(DeliveryStatus.DELIVERED);
    }

    // ---------------------------------------------------------------- "gitti mi bilmiyorum"

    @Test
    void timeoutAfterProviderSent_isReconciledAsSent_withoutSecondSms() {
        smsA.script(Outcome.TIMEOUT_AFTER_SEND);

        String id = send(request("CRITICAL", "u_1", "otp_login", Map.of("code", "111111"), List.of("SMS")));

        Delivery d = awaitStatus(id, Channel.SMS, DeliveryStatus.SENT);
        assertThat(d.provider()).isEqualTo("smsA");
        assertThat(smsA.handsetCount(id + ":SMS")).as("sağlayıcıya soruldu, tekrar gönderilmedi").isEqualTo(1);
        assertThat(smsB.handsetCount(id + ":SMS")).isZero();
    }

    @Test
    void otp_unknownAndProviderCannotBeQueried_isResent() {
        smsA.script(Outcome.TIMEOUT_BEFORE_SEND);
        smsA.setLookupUnavailable(true);

        String id = send(request("CRITICAL", "u_1", "otp_login", Map.of("code", "222222"), List.of("SMS")));

        Delivery d = awaitStatus(id, Channel.SMS, DeliveryStatus.SENT);
        assertThat(d.attempt()).isEqualTo(2);
        assertThat(smsA.handsetCount(id + ":SMS") + smsB.handsetCount(id + ":SMS")).isEqualTo(1);
    }

    @Test
    void campaign_unknownAndProviderCannotBeQueried_isAbandoned() {
        smsA.script(Outcome.TIMEOUT_BEFORE_SEND);
        smsA.setLookupUnavailable(true);

        Map<String, Object> req = request("MARKETING", "u_1", "promo_generic",
                Map.of("headline", "Kasım fırsatı!", "url", "https://px.example/k"), List.of("SMS"));
        req.put("campaignId", "cmp_demo");
        String id = send(req);

        Delivery d = awaitStatus(id, Channel.SMS, DeliveryStatus.FAILED);
        assertThat(d.lastError()).isEqualTo("unknown_abandoned");
        assertThat(smsA.handsetCount(id + ":SMS")).isZero();
    }

    // ---------------------------------------------------------------- sağlayıcı yedeği

    @Test
    void primarySmsProviderDown_otpFailsOverToSecondary() {
        smsA.script(Outcome.UNAVAILABLE);

        String id = send(request("CRITICAL", "u_1", "otp_login", Map.of("code", "333333"), List.of("SMS")));

        Delivery d = awaitStatus(id, Channel.SMS, DeliveryStatus.SENT);
        assertThat(d.provider()).isEqualTo("smsB");
        assertThat(smsB.handsetCount(id + ":SMS")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- push → SMS yedeği

    @Test
    void pushNotAcknowledged_smsFallbackCarriesSameCode() {
        Map<String, Object> req = request("CRITICAL", "u_1", "otp_login", Map.of("code", "444444"), List.of("PUSH", "SMS"));
        req.put("fallback", true);
        String id = send(req);

        awaitStatus(id, Channel.PUSH, DeliveryStatus.SENT);
        assertThat(delivery(id, Channel.SMS).status()).isEqualTo(DeliveryStatus.FALLBACK_WAIT);

        awaitStatus(id, Channel.SMS, DeliveryStatus.SENT);
        assertThat(fcm.handsetMessages().getLast().body()).isEqualTo("444444");
        assertThat(smsA.handsetMessages().getLast().body()).contains("444444");
    }

    @Test
    void pushAcknowledged_smsFallbackIsSkipped() {
        Map<String, Object> req = request("CRITICAL", "u_1", "otp_login", Map.of("code", "555555"), List.of("PUSH", "SMS"));
        req.put("fallback", true);
        String id = send(req);

        awaitStatus(id, Channel.PUSH, DeliveryStatus.SENT);
        http.postForObject("/deliveries/" + id + ":PUSH/ack", null, Map.class);

        Delivery sms = awaitStatus(id, Channel.SMS, DeliveryStatus.SKIPPED);
        assertThat(sms.lastError()).isEqualTo("primary_acknowledged");
        assertThat(smsA.handsetCount(id + ":SMS")).isZero();
    }

    // ---------------------------------------------------------------- izin, sessiz saat, kampanya

    @Test
    void marketing_withoutConsent_isNeverSent() {
        Map<String, Object> req = request("MARKETING", "u_2", "promo_generic",
                Map.of("headline", "Fırsat", "url", "https://px.example"), List.of("SMS"));
        req.put("campaignId", "cmp_demo");
        String id = send(req);

        Delivery d = awaitStatus(id, Channel.SMS, DeliveryStatus.SKIPPED);
        assertThat(d.lastError()).isEqualTo("no_marketing_consent");
    }

    @Test
    void marketing_inQuietHours_isScheduled_andConsentRevokedWhileWaiting_isNotSent() {
        // u_3 Londra'da: şu an sessiz saatin ortasında olacak şekilde ayarla
        LocalTime nowLondon = LocalTime.now(ZoneId.of("Europe/London")).withSecond(0).withNano(0);
        prefs("u_3", true, false, nowLondon.minusHours(1), nowLondon.plusHours(1));

        Map<String, Object> req = request("MARKETING", "u_3", "promo_generic",
                Map.of("headline", "Black Friday", "url", "https://px.example/bf"), List.of("PUSH"));
        req.put("campaignId", "cmp_demo");
        String id = send(req);

        Delivery scheduled = awaitStatus(id, Channel.PUSH, DeliveryStatus.SCHEDULED);
        assertThat(scheduled.notBefore()).isAfter(java.time.Instant.now().plus(Duration.ofMinutes(50)));

        // Zorluk #5: beklerken kullanıcı pazarlama iznini kapattı...
        prefs("u_3", false, false, nowLondon.minusHours(1), nowLondon.plusHours(1));
        // ...ve sessiz saat bitti (testte zamanı ileri saramadığımız için not_before'u şimdiye çekiyoruz)
        jdbc.sql("UPDATE delivery SET not_before = now() WHERE delivery_id = :id").param("id", id + ":PUSH").update();

        Delivery d = awaitStatus(id, Channel.PUSH, DeliveryStatus.SKIPPED);
        assertThat(d.lastError()).isEqualTo("consent_revoked");
        assertThat(fcm.handsetCount(id + ":PUSH")).isZero();
    }

    @Test
    void stoppedCampaign_messagesAreDropped() {
        String campaign = "cmp_" + UUID.randomUUID().toString().substring(0, 8);
        http.postForEntity("/campaigns", Map.of("campaignId", campaign, "name", "Yanlış kod"), Void.class);
        http.postForEntity("/campaigns/" + campaign + "/stop", null, Map.class);

        Map<String, Object> req = request("MARKETING", "u_1", "promo_generic",
                Map.of("headline", "İndirim kodu: YANLIS50", "url", "https://px.example"), List.of("PUSH", "SMS"));
        req.put("campaignId", campaign);
        String id = send(req);

        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .until(() -> deliveries.findByNotification(id).isEmpty());
        assertThat(fcm.handsetCount(id + ":PUSH") + smsA.handsetCount(id + ":SMS")).isZero();
    }

    @Test
    void notificationStatus_isQueryableForCustomerSupport() {
        String id = send(request("CRITICAL", "u_1", "otp_login", Map.of("code", "666666"), List.of("SMS")));
        awaitStatus(id, Channel.SMS, DeliveryStatus.SENT);

        Map<?, ?> body = http.getForObject("/notifications/" + id, Map.class);
        assertThat(body.get("priority")).isEqualTo("CRITICAL");
        assertThat((List<?>) body.get("deliveries")).singleElement()
                .satisfies(d -> assertThat(((Map<?, ?>) d).get("status")).isEqualTo("SENT"));
    }

    // ---------------------------------------------------------------- yardımcılar

    private Map<String, Object> request(String priority, String userId, String template, Map<String, String> params,
                                        List<String> channels) {
        Map<String, Object> r = new HashMap<>();
        r.put("idempotencyKey", "it:" + UUID.randomUUID());
        r.put("type", template.toUpperCase());
        r.put("priority", priority);
        r.put("userId", userId);
        r.put("templateId", template);
        r.put("params", params);
        r.put("channels", channels);
        return r;
    }

    private String send(Map<String, Object> req) {
        ResponseEntity<Map> res = http.postForEntity("/notifications", req, Map.class);
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return (String) res.getBody().get("notificationId");
    }

    private Delivery delivery(String notificationId, Channel channel) {
        return deliveries.find(DeliveryMessage.deliveryId(notificationId, channel)).orElse(null);
    }

    private Delivery awaitStatus(String notificationId, Channel channel, DeliveryStatus status) {
        return await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> delivery(notificationId, channel), d -> d != null && d.status() == status);
    }

    private void prefs(String userId, boolean consent, boolean sms, LocalTime quietStart, LocalTime quietEnd) {
        users.updatePreferences(userId, new UserRepository.PreferenceUpdate(consent, sms, true, true, quietStart, quietEnd));
        directory.invalidate(userId);
    }
}
