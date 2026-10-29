package io.paramix.notify.domain;

/** Teslimat durum makinesi. Geçişler {@code DeliveryRepository}'de koşullu UPDATE'lerle yapılır. */
public enum DeliveryStatus {
    PENDING, SCHEDULED, FALLBACK_WAIT, SENDING, SENT, DELIVERED, UNKNOWN, FAILED, SKIPPED
}
