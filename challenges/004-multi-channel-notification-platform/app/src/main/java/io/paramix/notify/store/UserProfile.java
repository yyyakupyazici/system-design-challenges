package io.paramix.notify.store;

import java.time.LocalTime;
import java.util.List;

import io.paramix.notify.domain.Channel;

/** Router'ın karar vermek için ihtiyaç duyduğu her şey: iletişim bilgisi, tercihler, push token'ları. */
public record UserProfile(
        String userId,
        String phone,
        String email,
        String locale,
        String timeZone,
        boolean marketingConsent,
        boolean smsEnabled,
        boolean emailEnabled,
        boolean pushEnabled,
        LocalTime quietStart,
        LocalTime quietEnd,
        List<String> pushTokens) {

    public boolean channelEnabled(Channel channel) {
        return switch (channel) {
            case SMS -> smsEnabled;
            case EMAIL -> emailEnabled;
            case PUSH -> pushEnabled;
        };
    }

    /** Kanalın alıcısı; yoksa null. Push'ta kullanıcının tüm cihaz token'ları virgülle. */
    public String recipient(Channel channel) {
        return switch (channel) {
            case SMS -> blankToNull(phone);
            case EMAIL -> blankToNull(email);
            case PUSH -> pushTokens == null || pushTokens.isEmpty() ? null : String.join(",", pushTokens);
        };
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }
}
