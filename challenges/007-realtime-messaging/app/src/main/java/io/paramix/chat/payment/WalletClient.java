package io.paramix.chat.payment;

/**
 * 006'daki cüzdan çekirdeğine giden arayüz. Sözleşme: aynı {@code idempotencyKey} ile kaç kez çağrılırsa çağrılsın
 * para BİR KEZ hareket eder, sonraki çağrılar ilk sonucu döner. Sohbetteki "Öde" butonunun iki kez ödememesinin
 * son savunma hattı burası.
 */
public interface WalletClient {

    record TransferResult(boolean ok, String transferId, String rejectReason) {
        public static TransferResult done(String id) {
            return new TransferResult(true, id, null);
        }

        public static TransferResult rejected(String reason) {
            return new TransferResult(false, null, reason);
        }
    }

    /** Cevap belirsiz: zaman aşımı, bağlantı koptu. Para gitmiş de olabilir, gitmemiş de. */
    class WalletUnavailableException extends RuntimeException {
        public WalletUnavailableException(String message) {
            super(message);
        }
    }

    TransferResult transfer(String idempotencyKey, long fromUserId, long toUserId, long amountKurus);
}
