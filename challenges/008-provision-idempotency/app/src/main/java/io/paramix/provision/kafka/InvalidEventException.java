package io.paramix.provision.kafka;

/** Çözülemeyen mesaj (bozuk JSON, event_id yok). Tekrar denemek düzeltmez: doğrudan DLT'ye gider. */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
