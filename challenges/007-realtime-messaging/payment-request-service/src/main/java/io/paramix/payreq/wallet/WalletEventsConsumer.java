package io.paramix.payreq.wallet;

import io.paramix.payreq.domain.PaymentRequestService;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 006'daki cüzdanın olayları. Transfer tamamlandı → isteği ödenmiş yap (sohbet dışından ödeme, belirsiz kalan ödeme);
 * iade edildi → REVERSED. En az bir kez gelir, senkron cevaptan önce de gelebilir: geçişler koşullu, tekrar etkisiz.
 */
@Component
public class WalletEventsConsumer {

    private static final String KEY_PREFIX = "payreq-";

    private final ObjectMapper json;
    private final WalletEventHandler handler;
    private final MeterRegistry meters;

    public WalletEventsConsumer(ObjectMapper json, WalletEventHandler handler, MeterRegistry meters) {
        this.json = json;
        this.handler = handler;
        this.meters = meters;
    }

    @KafkaListener(topics = "${payreq.topics.wallet}")
    public void onEvent(String payload) throws Exception {
        WalletEvent e = json.readValue(payload, WalletEvent.class);
        boolean changed = switch (String.valueOf(e.type())) {
            case "TRANSFER_COMPLETED" -> {
                Long id = paymentRequestIdOf(e.idempotencyKey());
                yield id != null && handler.completed(id, e.transferId(), e.amountKurus());
            }
            case "TRANSFER_REVERSED" -> handler.reversed(e.transferId(), e.reason());
            default -> false;
        };
        meters.counter("payreq.wallet_events", "type", String.valueOf(e.type()), "changed", String.valueOf(changed)).increment();
    }

    static Long paymentRequestIdOf(String key) {
        if (key == null || !key.startsWith(KEY_PREFIX)) {
            return null;                                        // para isteğinden gelmeyen transfer
        }
        try {
            return Long.parseLong(key.substring(KEY_PREFIX.length()));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    /** Servisin paket-içi metotlarına köprü (cüzdan paketi domain'in iç detaylarını bilmesin). */
    public interface WalletEventHandler {
        boolean completed(long paymentRequestId, String transferId, long amountKurus);

        boolean reversed(String transferId, String reason);
    }
}
