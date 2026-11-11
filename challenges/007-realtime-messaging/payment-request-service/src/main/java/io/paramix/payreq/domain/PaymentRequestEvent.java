package io.paramix.payreq.domain;

/**
 * "payment.events" topic'inin olayı. Sadece "ne oldu" değil, isteğin O ANKİ TAM GÖRÜNTÜSÜ:
 * dinleyen taraf (sohbet, bildirim, muhasebe) başka bir yere sormadan kendi görünümünü kurabilir,
 * ve olaylar sırasız gelse bile {@code version} ile doğru son durumu bulur.
 *
 * @param type CREATED | PAID | DECLINED | EXPIRED | REVERSED
 */
public record PaymentRequestEvent(String type, long paymentRequestId, int version, String status, long requesterId,
                                  long payerId, long amountKurus, String description, String originKind,
                                  String originRef, String reason, String occurredAt) {

    public static PaymentRequestEvent of(String type, PaymentRequest pr) {
        return new PaymentRequestEvent(type, pr.id(), pr.version(), pr.status(), pr.requesterId(), pr.payerId(),
                pr.amountKurus(), pr.description(), pr.originKind(), pr.originRef(), pr.reversalReason(),
                java.time.Instant.now().toString());
    }
}
