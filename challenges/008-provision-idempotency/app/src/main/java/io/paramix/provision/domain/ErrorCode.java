package io.paramix.provision.domain;

/** İş kuralı hataları. Deterministik oldukları için idempotency kaydına yazılır ve aynen tekrar oynatılır. */
public enum ErrorCode {
    UNKNOWN_ASSET(422, "Bilinmeyen varlık"),
    INVALID_AMOUNT(422, "Geçersiz tutar"),
    INVALID_TTL(422, "Geçersiz provizyon süresi"),
    OPERATION_TYPE_NOT_ALLOWED(422, "Bu işlem tipi bu uçtan açılamaz"),
    INSUFFICIENT_FUNDS(422, "Bakiye yetersiz"),
    PROVISION_ALREADY_EXISTS(409, "Bu işlem için zaten bir provizyon var"),
    PROVISION_NOT_FOUND(404, "Provizyon bulunamadı"),
    PROVISION_NOT_HELD(409, "Provizyon artık bloke değil"),
    PROVISION_MANAGED_BY_WITHDRAWAL(409, "Çekim provizyonu sadece çekim süreci tarafından kapatılır"),
    CAPTURE_EXCEEDS_HOLD(422, "Capture tutarı provizyondan büyük"),
    DEPOSIT_REF_CONFLICT(409, "Bu dış referans farklı bir yatırım için kullanılmış"),
    WITHDRAWAL_NOT_FOUND(404, "Çekim bulunamadı");

    private final int status;
    private final String title;

    ErrorCode(int status, String title) {
        this.status = status;
        this.title = title;
    }

    public int status() {
        return status;
    }

    public String title() {
        return title;
    }
}
