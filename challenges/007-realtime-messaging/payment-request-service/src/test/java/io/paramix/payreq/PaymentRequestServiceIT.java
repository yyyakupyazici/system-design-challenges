package io.paramix.payreq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import io.paramix.payreq.wallet.FakeWalletClient;
import io.paramix.payreq.wallet.FakeWalletClient.Mode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Servis tek başına: HTTP API, durum makinesi, cüzdan, reconciler, süre dolumu, wallet.events ve yayınladığı
 * payment.events olayları. Sohbet servisi yok; olayları test bir Kafka tüketicisiyle okur.
 */
class PaymentRequestServiceIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static EmbeddedKafkaKraftBroker kafka;
    private static ConfigurableApplicationContext app;
    private static int port;
    private static final List<JsonNode> events = new CopyOnWriteArrayList<>();
    private static final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    static void start() {
        assumeTrue(PayReqTestInfra.available(), "PostgreSQL yok (Docker ya da IT_PG_URL)");
        if (app != null) {
            return;
        }
        kafka = new EmbeddedKafkaKraftBroker(1, 3, "payment.events", "wallet.events");
        kafka.afterPropertiesSet();
        app = PaymentRequestApplication.run(
                "--server.port=0",
                "--spring.datasource.url=" + PayReqTestInfra.url(),
                "--spring.datasource.username=" + PayReqTestInfra.user(),
                "--spring.datasource.password=" + PayReqTestInfra.password(),
                "--spring.kafka.bootstrap-servers=" + kafka.getBrokersAsString(),
                "--payreq.topics.partitions=3",
                "--payreq.reconcile-after=1s",
                "--payreq.reconcile-every=300ms");
        port = Integer.parseInt(app.getEnvironment().getProperty("local.server.port"));
        Thread reader = new Thread(PaymentRequestServiceIT::readEvents, "payment-events-reader");
        reader.setDaemon(true);
        reader.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.close();
            kafka.destroy();
        }));
    }

    private static void readEvents() {
        Map<String, Object> cfg = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-reader",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cfg)) {
            c.subscribe(List.of("payment.events"));
            while (true) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(200))) {
                    JsonNode n = JSON.readTree(r.value());
                    assertThat(r.key()).as("anahtar = istek id (aynı isteğin olayları sıralı)").isEqualTo(n.get("paymentRequestId").asText());
                    events.add(n);
                }
            }
        } catch (Exception e) {
            // JVM kapanıyor
        }
    }

    // ------------------------------------------------------------------ yardımcılar

    private static long newUser() {
        return ThreadLocalRandom.current().nextLong(1_000_000, 1_000_000_000);
    }

    private static FakeWalletClient wallet() {
        return app.getBean(FakeWalletClient.class);
    }

    private static JdbcClient jdbc() {
        return app.getBean(JdbcClient.class);
    }

    private record Resp(int status, JsonNode body) {
    }

    private static Resp call(String method, String path, long userId, Map<String, String> headers, Object body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("X-User-Id", String.valueOf(userId)).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        headers.forEach(b::header);
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new Resp(r.statusCode(), r.body().isEmpty() ? null : JSON.readTree(r.body()));
    }

    private static JsonNode create(long requester, long payer, long amount, String key) throws Exception {
        Resp r = call("POST", "/v1/payment-requests", requester, Map.of("Idempotency-Key", key),
                Map.of("payerId", payer, "amountKurus", amount, "description", "Akşam yemeği",
                        "origin", Map.of("kind", "CHAT", "ref", "conv-42")));
        assertThat(r.status()).isEqualTo(201);
        return r.body();
    }

    private static long create(long requester, long payer, long amount) throws Exception {
        return create(requester, payer, amount, UUID.randomUUID().toString()).get("id").asLong();
    }

    private static String pay(long payer, long id) throws Exception {
        return call("POST", "/v1/payment-requests/" + id + "/pay", payer, Map.of(), null).body().get("status").asText();
    }

    private static String status(long id) {
        return jdbc().sql("SELECT status FROM payment_requests WHERE id = ?").param(id).query(String.class).single();
    }

    private static List<JsonNode> eventsOf(long id) {
        return events.stream().filter(e -> e.get("paymentRequestId").asLong() == id).toList();
    }

    private static List<String> typesOf(long id) {
        return eventsOf(id).stream().map(e -> e.get("type").asText()).toList();
    }

    private static void awaitTypes(long id, String... types) {
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(typesOf(id)).containsExactly(types));
    }

    @SuppressWarnings("unchecked")
    private static void walletPublishes(String type, String transferId, String key, long amount, String reason) throws Exception {
        Map<String, Object> e = new HashMap<>(Map.of("type", type, "transferId", transferId, "amountKurus", amount,
                "fromUserId", 1, "toUserId", 2));
        if (key != null) {
            e.put("idempotencyKey", key);
        }
        if (reason != null) {
            e.put("reason", reason);
        }
        app.getBean(KafkaTemplate.class).send("wallet.events", transferId, JSON.writeValueAsString(e)).get();
    }

    // ------------------------------------------------------------------ testler

    @Test
    void createIsIdempotentAndPublishesOneCreatedEventWithTheFullSnapshot() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        String key = UUID.randomUUID().toString();
        JsonNode first = create(ali, ayse, 25_000, key);
        JsonNode again = create(ali, ayse, 25_000, key);                       // ağ koptu, uygulama tekrar gönderdi
        long id = first.get("id").asLong();
        assertThat(again.get("id").asLong()).isEqualTo(id);
        assertThat(first.get("status").asText()).isEqualTo("PENDING");

        awaitTypes(id, "CREATED");
        JsonNode e = eventsOf(id).get(0);
        assertThat(e.get("version").asInt()).isEqualTo(1);
        assertThat(e.get("amountKurus").asLong()).isEqualTo(25_000);
        assertThat(e.get("originKind").asText()).isEqualTo("CHAT");
        assertThat(e.get("originRef").asText()).isEqualTo("conv-42");
        assertThat(e.get("payerId").asLong()).isEqualTo(ayse);
    }

    @Test
    void validationAndAuthorization() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        assertThat(call("POST", "/v1/payment-requests", ali, Map.of("Idempotency-Key", "k"),
                Map.of("payerId", ali, "amountKurus", 100)).status()).isEqualTo(400);                  // kendinden
        assertThat(call("POST", "/v1/payment-requests", ali, Map.of("Idempotency-Key", "k"),
                Map.of("payerId", ayse, "amountKurus", 0)).status()).isEqualTo(400);                   // sıfır tutar
        long id = create(ali, ayse, 1_000);
        assertThat(call("POST", "/v1/payment-requests/" + id + "/pay", ali, Map.of(), null).status())
                .as("isteyen kendi isteğini ödeyemez").isEqualTo(403);
        assertThat(call("GET", "/v1/payment-requests/" + id, newUser(), Map.of(), null).status())
                .as("üçüncü kişi isteği göremez").isEqualTo(404);
        assertThat(call("GET", "/v1/payment-requests/" + id, ayse, Map.of(), null).status()).isEqualTo(200);
    }

    @Test
    void payingFromTwoDevicesAtOnceMovesMoneyOnce() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long id = create(ali, ayse, 25_000);
        int before = wallet().moneyMovements();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Callable<String>> both = List.of(() -> pay(ayse, id), () -> pay(ayse, id));
        List<String> results = new ArrayList<>();
        for (Future<String> f : pool.invokeAll(both)) {
            results.add(f.get());
        }
        pool.shutdown();

        assertThat(results).containsOnlyOnce("PAID");
        assertThat(results).filteredOn(r -> !r.equals("PAID")).allMatch(r -> r.equals("ALREADY_PAID") || r.equals("PROCESSING"));
        assertThat(wallet().moneyMovements() - before).isEqualTo(1);
        awaitTypes(id, "CREATED", "PAID");
        // PENDING(1) → PROCESSING(2) → PAID(3): PROCESSING olay üretmez ama sürümü artırır
        assertThat(eventsOf(id).get(1).get("version").asInt()).isEqualTo(3);
        assertThat(pay(ayse, id)).isEqualTo("ALREADY_PAID");
    }

    @Test
    void lostWalletResponseIsCompletedByTheReconcilerWithoutMovingMoneyTwice() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long id = create(ali, ayse, 7_500);
        int before = wallet().moneyMovements();

        wallet().script(Mode.TIMEOUT_AFTER_TRANSFER);                          // para gitti, cevap kayboldu
        assertThat(pay(ayse, id)).isEqualTo("PROCESSING");
        assertThat(status(id)).isEqualTo("PROCESSING");

        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> "PAID".equals(status(id)));
        awaitTypes(id, "CREATED", "PAID");
        assertThat(wallet().moneyMovements() - before).isEqualTo(1);
    }

    @Test
    void insufficientFundsReleasesTheRequestSoItCanBePaidLater() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long id = create(ali, ayse, 99_000);
        wallet().script(Mode.INSUFFICIENT_FUNDS);
        assertThat(pay(ayse, id)).isEqualTo("FAILED:INSUFFICIENT_FUNDS");
        assertThat(status(id)).isEqualTo("PENDING");
        assertThat(pay(ayse, id)).isEqualTo("PAID");                          // para yükledi, tekrar denedi
        awaitTypes(id, "CREATED", "PAID");
    }

    @Test
    void declineAndExpiryAreFinal() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long declined = create(ali, ayse, 1_000);
        assertThat(call("POST", "/v1/payment-requests/" + declined + "/decline", ayse, Map.of(), null)
                .body().get("declined").asBoolean()).isTrue();
        assertThat(pay(ayse, declined)).isEqualTo("DECLINED");
        awaitTypes(declined, "CREATED", "DECLINED");

        long expiring = create(ali, ayse, 2_000);
        jdbc().sql("UPDATE payment_requests SET expires_at = now() - interval '1 second' WHERE id = ?").param(expiring).update();
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> "EXPIRED".equals(status(expiring)));
        assertThat(pay(ayse, expiring)).isEqualTo("EXPIRED");
        awaitTypes(expiring, "CREATED", "EXPIRED");
    }

    @Test
    void walletEventsPayOutsideTheFlowAndReverseLater() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long id = create(ali, ayse, 120_000);

        // Ayşe isteği cüzdan ekranından ödedi; olay en az bir kez gelir (iki kez yayınla)
        walletPublishes("TRANSFER_COMPLETED", "trf_outside_" + id, "payreq-" + id, 120_000, null);
        walletPublishes("TRANSFER_COMPLETED", "trf_outside_" + id, "payreq-" + id, 120_000, null);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> "PAID".equals(status(id)));

        // Günler sonra itiraz
        walletPublishes("TRANSFER_REVERSED", "trf_outside_" + id, null, 120_000, "CHARGEBACK");
        walletPublishes("TRANSFER_REVERSED", "trf_outside_" + id, null, 120_000, "CHARGEBACK");
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> "REVERSED".equals(status(id)));
        awaitTypes(id, "CREATED", "PAID", "REVERSED");
        List<Integer> versions = eventsOf(id).stream().map(e -> e.get("version").asInt()).toList();
        assertThat(versions).isSorted().doesNotHaveDuplicates();
        assertThat(eventsOf(id).get(2).get("reason").asText()).isEqualTo("CHARGEBACK");
    }

    @Test
    void walletEventArrivingBeforeTheSynchronousResponseWinsAndNothingIsDuplicated() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long id = create(ali, ayse, 4_500);
        int before = wallet().moneyMovements();

        wallet().script(Mode.SLOW_OK);                                         // cevap 1,5 sn gecikecek
        Future<String> paying = Executors.newSingleThreadExecutor().submit(() -> pay(ayse, id));
        Awaitility.await().atMost(Duration.ofSeconds(1)).until(() -> "PROCESSING".equals(status(id)));
        walletPublishes("TRANSFER_COMPLETED", "trf_event_" + id, "payreq-" + id, 4_500, null);

        assertThat(paying.get()).isEqualTo("PAID");
        awaitTypes(id, "CREATED", "PAID");
        Thread.sleep(500);
        assertThat(typesOf(id)).containsExactly("CREATED", "PAID");
        assertThat(jdbc().sql("SELECT wallet_transfer_id FROM payment_requests WHERE id = ?").param(id).query(String.class).single())
                .as("durumu olay yolu yazdı").isEqualTo("trf_event_" + id);
        assertThat(wallet().moneyMovements() - before).isEqualTo(1);
    }

    @Test
    void mismatchedOrUnrelatedWalletEventsChangeNothing() throws Exception {
        long ali = newUser();
        long ayse = newUser();
        long id = create(ali, ayse, 50_000);
        walletPublishes("TRANSFER_COMPLETED", "trf_wrong_" + id, "payreq-" + id, 5_000, null);     // 500 TL istendi, 50 TL geldi
        walletPublishes("TRANSFER_COMPLETED", "trf_other_" + id, "qr-payment-77", 50_000, null);   // başka bir ödeme
        walletPublishes("TRANSFER_REVERSED", "trf_unknown_" + id, null, 50_000, "FRAUD");          // bizim olmayan transfer
        Thread.sleep(1_500);
        assertThat(status(id)).isEqualTo("PENDING");
        assertThat(typesOf(id)).containsExactly("CREATED");
    }
}
