package io.paramix.payreq.wallet;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 006'daki cüzdanın "wallet.events" topic'ine yayınladığı olay (cüzdan bunu transfer kaydıyla aynı transaction'da
 * outbox'a yazar). Sohbet servisi sadece ihtiyacı olan alanları okur.
 *
 * @param type           TRANSFER_COMPLETED | TRANSFER_REVERSED (diğerleri yok sayılır)
 * @param idempotencyKey transferi başlatanın anahtarı; para isteğinden gelen ödemelerde "payreq-&lt;id&gt;"
 * @param reason         iade sebebi (CHARGEBACK, FRAUD, ...)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WalletEvent(String type, String transferId, String idempotencyKey, long fromUserId, long toUserId,
                          long amountKurus, String reason) {
}
