package io.paramix.llmgw.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.stream.Collectors;

import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.ClientProperties;
import io.paramix.llmgw.core.GatewayException;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * {@code Authorization: Bearer <anahtar>} → ekip + uygulama. Ekipler sağlayıcı anahtarlarını hiç görmez;
 * sadece gateway anahtarı alır, gateway anahtarı iptal edilebilir ve ekip bazında izlenir.
 */
@Component
public class ApiKeyAuthenticator {

    private final Map<String, ClientProperties> byHash;

    public ApiKeyAuthenticator(GatewayProperties props) {
        this.byHash = props.clients().values().stream()
                .collect(Collectors.toUnmodifiableMap(c -> c.keySha256().toLowerCase(), c -> c));
    }

    public ClientContext authenticate(String authorization, String userId, String dataClass, String sessionId) {
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            throw new GatewayException(HttpStatus.UNAUTHORIZED, "missing_api_key", "Authorization: Bearer <anahtar> gerekli");
        }
        ClientProperties c = byHash.get(sha256(authorization.substring(7).trim()));
        if (c == null) {
            throw new GatewayException(HttpStatus.UNAUTHORIZED, "invalid_api_key", "Geçersiz API anahtarı");
        }
        boolean confidential = "confidential".equalsIgnoreCase(dataClass) || "gizli".equalsIgnoreCase(dataClass);
        String user = userId == null || userId.isBlank() ? "anonymous" : userId;
        return new ClientContext(c.team(), c.app(), c.appType(), c.cacheable(), user, confidential,
                sessionId == null || sessionId.isBlank() ? null : sessionId);
    }

    public static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
