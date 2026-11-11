package io.paramix.chat.payment;

import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Cüzdan → sohbet yönü. 006'daki cüzdanın olaylarını kendi consumer group'unda ("chat-payments") dinler.
 * Bu topic'i fraud, muhasebe ve bildirim de dinliyor; her biri kendi hızında, kendi offset'iyle.
 *
 * <p>Olaylar en az bir kez gelir (cüzdanın outbox relay'i tekrar gönderebilir) ve senkron cevaptan önce de
 * gelebilir. İkisi de güvenli: durum geçişleri koşullu, sistem mesajları deterministik id'li.
 */
@Component
@ConditionalOnProperty(name = "chat.roles.payment", havingValue = "true", matchIfMissing = true)
public class WalletEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(WalletEventsConsumer.class);
    private static final String KEY_PREFIX = "payreq-";

    private final Json json;
    private final PaymentRequestService payments;
    private final MeterRegistry meters;

    public WalletEventsConsumer(Json json, PaymentRequestService payments, MeterRegistry meters) {
        this.json = json;
        this.payments = payments;
        this.meters = meters;
    }

    @KafkaListener(topics = "${chat.wallet.events-topic:wallet.events}", groupId = "chat-payments")
    public void onEvent(String payload) throws Exception {
        WalletEvent e = json.mapper().readValue(payload, WalletEvent.class);
        switch (String.valueOf(e.type())) {
            case "TRANSFER_COMPLETED" -> {
                Long prId = paymentRequestIdOf(e.idempotencyKey());
                if (prId == null) {
                    return;                                         // para isteğinden gelmeyen transfer: bizi ilgilendirmez
                }
                boolean changed = payments.onTransferCompleted(prId, e.transferId(), e.amountKurus());
                meters.counter("chat.wallet_events", "type", "completed", "changed", String.valueOf(changed)).increment();
            }
            case "TRANSFER_REVERSED" -> {
                boolean changed = payments.onTransferReversed(e.transferId(), e.reason());
                meters.counter("chat.wallet_events", "type", "reversed", "changed", String.valueOf(changed)).increment();
            }
            default -> log.debug("İlgisiz cüzdan olayı: {}", e.type());
        }
    }

    static Long paymentRequestIdOf(String idempotencyKey) {
        if (idempotencyKey == null || !idempotencyKey.startsWith(KEY_PREFIX)) {
            return null;
        }
        try {
            return Long.parseLong(idempotencyKey.substring(KEY_PREFIX.length()));
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
