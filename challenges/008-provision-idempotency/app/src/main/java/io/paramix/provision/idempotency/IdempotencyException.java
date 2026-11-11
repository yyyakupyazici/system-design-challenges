package io.paramix.provision.idempotency;

import java.util.Map;

/**
 * Idempotency katmanının reddettiği istekler. Bunlar idempotency kaydına YAZILMAZ: istemci düzeltip
 * (yeni key, doğru payload ya da biraz bekleyip) tekrar deneyebilir.
 */
public class IdempotencyException extends RuntimeException {

    public enum Reason {
        /** Header yok. */
        KEY_MISSING(400, "Idempotency-Key header'ı zorunlu"),
        /** UUIDv7 değil. */
        KEY_INVALID(400, "Idempotency-Key bir UUIDv7 olmalı"),
        /** Key kabul penceresinden eski (kaydı silinmiş olabilir) ya da çok ileri tarihli. */
        KEY_EXPIRED(400, "Idempotency-Key kabul penceresinin dışında"),
        /** Aynı key daha önce FARKLI bir istekle kullanılmış. */
        KEY_REUSED(422, "Idempotency-Key farklı bir istekle kullanılmış"),
        /** Aynı key ile ilk istek hâlâ işleniyor. */
        REQUEST_IN_PROGRESS(409, "Aynı Idempotency-Key ile bir istek hâlâ işleniyor");

        private final int status;
        private final String title;

        Reason(int status, String title) {
            this.status = status;
            this.title = title;
        }

        public int status() {
            return status;
        }

        public String title() {
            return title;
        }

        public String code() {
            return "IDEMPOTENCY_" + name();
        }
    }

    private final Reason reason;
    private final Map<String, Object> context;

    public IdempotencyException(Reason reason, String detail) {
        this(reason, detail, Map.of());
    }

    public IdempotencyException(Reason reason, String detail, Map<String, Object> context) {
        super(detail);
        this.reason = reason;
        this.context = context;
    }

    public Reason reason() {
        return reason;
    }

    public Map<String, Object> context() {
        return context;
    }
}
