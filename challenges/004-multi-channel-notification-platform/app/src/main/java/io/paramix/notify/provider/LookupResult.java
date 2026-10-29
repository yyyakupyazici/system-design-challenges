package io.paramix.notify.provider;

/** "Bu clientRef ile gönderdim mi?" sorusunun cevabı. */
public sealed interface LookupResult {

    record Found(String providerMsgId) implements LookupResult {
    }

    record NotFound() implements LookupResult {
    }

    /** Sağlayıcı sorguyu cevaplayamadı: gerçekten bilmiyoruz → öncelik politikası karar verir. */
    record Unavailable(String detail) implements LookupResult {
    }
}
