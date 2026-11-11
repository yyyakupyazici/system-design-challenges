package io.paramix.chat.paymentcard;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.message.ConversationRepository.Conversation;
import io.paramix.chat.message.MessagePoster;
import io.paramix.chat.paymentcard.PaymentCardRepository.Change;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * payment.events → sohbetteki kart. Sohbet para mantığı bilmez; sadece olayı EKRANA çevirir.
 *
 * <pre>
 *  olay (origin CHAT) ─┬─ kart yok      → kartı yaz + sohbete kart mesajı (gönderen = isteyen)
 *                      │                  + olay CREATED değilse durum mesajı da (sırası karışmış: önce "ödendi" geldi)
 *                      ├─ daha yeni     → kartı güncelle + durum mesajı ("✅ 250,00 TL ödendi")
 *                      └─ eski / tekrar → hiçbir şey (STALE)
 *  hepsi tek transaction: kart + mesaj + outbox birlikte yazılır ya da hiçbiri
 * </pre>
 *
 * Güvenlik: kaynağı (originRef) isteyen kişi belirler. İsteyen ve ödeyen o sohbetin üyesi değilse kart YAZILMAZ;
 * yoksa biri başkasının sohbetine kart düşürebilirdi.
 */
@Component
@ConditionalOnProperty(name = "chat.roles.payment-cards", havingValue = "true")
public class PaymentCardsConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCardsConsumer.class);
    private static final Locale TR = Locale.forLanguageTag("tr-TR");

    private final ObjectMapper json;
    private final PaymentCardRepository cards;
    private final ConversationRepository conversations;
    private final MessagePoster poster;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;

    public PaymentCardsConsumer(ObjectMapper json, PaymentCardRepository cards, ConversationRepository conversations,
                                MessagePoster poster, TransactionTemplate tx, MeterRegistry meters) {
        this.json = json;
        this.cards = cards;
        this.conversations = conversations;
        this.poster = poster;
        this.tx = tx;
        this.meters = meters;
    }

    @KafkaListener(topics = "${chat.payment-cards.topic:payment.events}", groupId = "chat-payment-cards")
    public void onEvent(String payload) throws Exception {
        PaymentRequestEvent e = json.readValue(payload, PaymentRequestEvent.class);
        meters.counter("chat.payment_cards.events", "result", handle(e)).increment();
    }

    String handle(PaymentRequestEvent e) {
        if (!"CHAT".equals(e.originKind())) {
            return "not_chat";                                  // ödeme linki, fatura...: sohbetle ilgisi yok
        }
        long conversationId;
        try {
            conversationId = Long.parseLong(e.originRef());
        } catch (NumberFormatException ex) {
            return "bad_ref";
        }
        if (!cards.exists(e.paymentRequestId()) && !allowed(conversationId, e)) {
            log.warn("Para isteği {} sohbet {} için reddedildi: isteyen ya da ödeyen sohbetin üyesi değil",
                    e.paymentRequestId(), conversationId);
            return "rejected";
        }
        return tx.execute(s -> {
            Change change = cards.upsert(e, conversationId);
            if (change == Change.STALE) {
                return "stale";
            }
            if (change == Change.CREATED) {
                poster.post(conversationId, e.requesterId(), id(e.paymentRequestId(), "CARD"), "PAYMENT_REQUEST",
                        e.description() == null || e.description().isBlank() ? "Para isteği" : e.description(),
                        e.paymentRequestId(), money(e.amountKurus()) + " para isteği");
            }
            if (!"CREATED".equals(e.type())) {
                poster.post(conversationId, 0, id(e.paymentRequestId(), e.type()), "SYSTEM", statusText(e),
                        e.paymentRequestId(), statusText(e));
            }
            return change.name().toLowerCase(Locale.ROOT);
        });
    }

    private boolean allowed(long conversationId, PaymentRequestEvent e) {
        Conversation c = conversations.find(conversationId).orElse(null);
        return c != null && !"BROADCAST".equals(c.kind())
                && conversations.membership(conversationId, e.requesterId()).isPresent()
                && conversations.membership(conversationId, e.payerId()).isPresent();
    }

    private static UUID id(long paymentRequestId, String what) {
        return UUID.nameUUIDFromBytes(("payreq:" + paymentRequestId + ":" + what).getBytes(StandardCharsets.UTF_8));
    }

    static String money(long kurus) {
        return String.format(TR, "%,.2f TL", kurus / 100.0);
    }

    static String statusText(PaymentRequestEvent e) {
        String m = money(e.amountKurus());
        return switch (e.type()) {
            case "PAID" -> "✅ " + m + " ödendi";
            case "DECLINED" -> "❌ " + m + " para isteği reddedildi";
            case "EXPIRED" -> "⌛ " + m + " para isteğinin süresi doldu";
            case "REVERSED" -> "↩️ " + m + " ödeme iade edildi" + (e.reason() == null ? "" : " (" + e.reason() + ")");
            default -> "Para isteği güncellendi: " + e.status();
        };
    }
}
