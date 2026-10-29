package io.paramix.notify.domain;

/** Bildirim önceliği. Kuyruk, worker havuzu ve sağlayıcı kotası bu eksende izole edilir. */
public enum Priority {
    CRITICAL,       // OTP, şüpheli işlem uyarısı
    TRANSACTIONAL,  // "Hesabına 500 TL geldi"
    MARKETING;      // kampanyalar

    public String topicSuffix() {
        return name().toLowerCase();
    }
}
