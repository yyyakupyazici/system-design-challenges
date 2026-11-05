package io.paramix.llmgw.auth;

import io.paramix.llmgw.config.GatewayProperties.AppType;

/**
 * İsteği kimin yaptığı.
 *
 * @param confidential "gizli" veri sınıfı: sadece kendi GPU kümemizdeki (internal) modele gidebilir
 * @param sessionId sohbet oturumu; varsa ve uygulama CHAT ise sağlayıcı tarafı prompt caching açılır
 */
public record ClientContext(String team, String app, AppType appType, boolean appCacheable,
                            String userId, boolean confidential, String sessionId) {
}
