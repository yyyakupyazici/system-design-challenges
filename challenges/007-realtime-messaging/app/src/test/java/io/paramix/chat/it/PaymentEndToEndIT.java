package io.paramix.chat.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.paramix.payreq.wallet.FakeWalletClient;
import io.paramix.payreq.wallet.FakeWalletClient.Mode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Uçtan uca, iki gerçek servis: istemci para isteğini payment-request-service'e HTTP ile gönderir ve öder;
 * sohbet servisi sadece payment.events'i dinleyerek kartı yazar ve günceller.
 *
 * <pre>
 *  Ali'nin uygulaması ──POST /v1/payment-requests──▶ payment-request-service ──payment.events──▶ sohbet ──▶ kart
 *  Ayşe'nin uygulaması ─POST /{id}/pay─────────────▶ payment-request-service ──payment.events──▶ sohbet ──▶ "✅ ödendi"
 * </pre>
 */
class PaymentEndToEndIT extends AbstractChatIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static JsonNode payments(String method, String path, long user, String idemKey, Object body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + ChatCluster.port(cluster.payments()) + path))
                .header("X-User-Id", String.valueOf(user)).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        if (idemKey != null) {
            b.header("Idempotency-Key", idemKey);
        }
        return JSON.readTree(HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString()).body());
    }

    private static long requestMoney(long from, long payer, long conv, long amount) throws Exception {
        return payments("POST", "/v1/payment-requests", from, UUID.randomUUID().toString(), Map.of("payerId", payer,
                "amountKurus", amount, "description", "Akşam yemeği",
                "origin", Map.of("kind", "CHAT", "ref", String.valueOf(conv)))).get("id").asLong();
    }

    private static String pay(long payer, long pr) throws Exception {
        return payments("POST", "/v1/payment-requests/" + pr + "/pay", payer, null, null).get("status").asText();
    }

    private static FakeWalletClient wallet() {
        return bean(cluster.payments(), FakeWalletClient.class);
    }

    private static boolean isCard(JsonNode f, long pr) {
        return f.at("/message/kind").asText().equals("PAYMENT_REQUEST") && f.at("/message/paymentRequest/id").asLong() == pr;
    }

    private static boolean isStatus(JsonNode f, long pr) {
        return f.at("/message/kind").asText().equals("SYSTEM") && f.at("/message/paymentRequest/id").asLong() == pr;
    }

    @Test
    void requestAndPayFromTwoDevicesAtOnceTheChatShowsOneCardAndOnePaidMessage() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portB, ali, "phone");
        TestClient aysePhone = connect(portA, ayse, "phone");
        TestClient ayseWeb = connect(portB, ayse, "web");

        long pr = requestMoney(ali, ayse, conv, 25_000);
        JsonNode card = aysePhone.await("MSG", f -> isCard(f, pr)).get("message");
        assertThat(card.at("/paymentRequest/status").asText()).isEqualTo("PENDING");
        assertThat(card.get("body").asText()).isEqualTo("Akşam yemeği");
        aliC.await("MSG", f -> isCard(f, pr));                      // isteyenin kendi cihazına da düşer

        int before = wallet().moneyMovements();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Callable<String>> both = List.of(() -> pay(ayse, pr), () -> pay(ayse, pr));   // telefon + web aynı anda
        List<String> results = new ArrayList<>();
        for (Future<String> f : pool.invokeAll(both)) {
            results.add(f.get());
        }
        pool.shutdown();
        assertThat(results).containsOnlyOnce("PAID");
        assertThat(wallet().moneyMovements() - before).isEqualTo(1);

        JsonNode sys = aliC.await("MSG", f -> isStatus(f, pr)).get("message");
        assertThat(sys.get("body").asText()).isEqualTo("✅ 250,00 TL ödendi");
        assertThat(sys.at("/paymentRequest/status").asText()).isEqualTo("PAID");
        ayseWeb.await("MSG", f -> isStatus(f, pr));
        Thread.sleep(500);
        assertThat(aliC.all("MSG").stream().filter(f -> isStatus(f, pr)).count()).isEqualTo(1);
    }

    /** Cüzdanın cevabı kayboldu: ödeme servisinin reconciler'ı tamamlar, sohbet sonucu olaydan öğrenir. */
    @Test
    void lostWalletResponseEndsAsPaidInTheChat() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        long pr = requestMoney(ali, ayse, conv, 7_500);
        aliC.await("MSG", f -> isCard(f, pr));

        wallet().script(Mode.TIMEOUT_AFTER_TRANSFER);
        assertThat(pay(ayse, pr)).isEqualTo("PROCESSING");
        JsonNode sys = aliC.await("MSG", f -> isStatus(f, pr), Duration.ofSeconds(15)).get("message");
        assertThat(sys.get("body").asText()).isEqualTo("✅ 75,00 TL ödendi");
    }

    @Test
    void declineIsShownInTheChat() throws Exception {
        long ali = user("Ali");
        long ayse = user("Ayşe");
        long conv = direct(ali, ayse);
        TestClient aliC = connect(portA, ali, "phone");
        long pr = requestMoney(ali, ayse, conv, 3_000);
        aliC.await("MSG", f -> isCard(f, pr));
        assertThat(payments("POST", "/v1/payment-requests/" + pr + "/decline", ayse, null, null).get("declined").asBoolean()).isTrue();
        assertThat(aliC.await("MSG", f -> isStatus(f, pr)).at("/message/body").asText()).isEqualTo("❌ 30,00 TL para isteği reddedildi");
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> "DECLINED".equals(
                jdbc().sql("SELECT status FROM payment_cards WHERE payment_request_id = ?").param(pr).query(String.class).single()));
    }
}
