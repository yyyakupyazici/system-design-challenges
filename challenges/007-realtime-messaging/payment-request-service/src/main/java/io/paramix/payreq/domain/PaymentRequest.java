package io.paramix.payreq.domain;

/**
 * @param originKind CHAT | LINK | INVOICE | APP — isteğin nereden oluşturulduğu (kurallar her yerde aynı)
 * @param originRef  CHAT ise sohbet id'si
 * @param version    her durum geçişinde artar; olayı alan taraf eski sürümü yok sayar
 */
public record PaymentRequest(long id, long requesterId, long payerId, long amountKurus, String description,
                             String originKind, String originRef, String status, int version,
                             String walletTransferId, String reversalReason, String expiresAt) {
}
