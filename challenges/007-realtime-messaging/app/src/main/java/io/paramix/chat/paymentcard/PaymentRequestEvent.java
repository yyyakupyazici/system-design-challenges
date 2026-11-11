package io.paramix.chat.paymentcard;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * payment-request-service'in yayınladığı olay (sözleşme: payment.events). Sohbet bu sınıfı kendisi tanımlar, servisin
 * kodunu import etmez: iki servis sadece JSON sözleşmesiyle bağlı. Bilinmeyen alanlar yok sayılır (servis yeni alan
 * eklerse sohbet bozulmaz).
 *
 * @param version   her durum geçişinde artar; kart sadece daha büyük sürümle güncellenir
 * @param originRef originKind = CHAT ise sohbet id'si
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentRequestEvent(String type, long paymentRequestId, int version, String status, long requesterId,
                                  long payerId, long amountKurus, String description, String originKind,
                                  String originRef, String reason, String occurredAt) {
}
