package io.paramix.notify.api;

import java.util.List;
import java.util.Map;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.Priority;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Ekiplerin gönderdiği istek.
 *
 * @param idempotencyKey çağıranın anlamlı anahtarı, ör. {@code payments:tx_991:credited}, {@code otp:login_7781}.
 *                       Timeout sonrası retry aynı anahtarla gelir → yeni bildirim oluşmaz.
 * @param channels       boşsa önceliğin varsayılan kanalları
 * @param fallback       true ise kanallar sırayla: ilki hemen, sonrakiler ack gelmezse (ör. önce push, sonra SMS)
 */
public record NotificationRequest(
        @NotBlank @Size(max = 200) String idempotencyKey,
        @NotBlank String type,
        @NotNull Priority priority,
        @NotBlank String userId,
        @NotBlank String templateId,
        Map<String, String> params,
        List<Channel> channels,
        boolean fallback,
        String campaignId) {
}
