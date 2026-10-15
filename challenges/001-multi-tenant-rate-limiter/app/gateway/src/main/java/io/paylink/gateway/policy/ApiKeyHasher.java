package io.paylink.gateway.policy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

/** API key'ler DB'de düz metin değil SHA-256 hash olarak tutulur. */
public final class ApiKeyHasher {

    private static final SecureRandom RANDOM = new SecureRandom();

    private ApiKeyHasher() {
    }

    public static String sha256Hex(String apiKey) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(apiKey.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 her JVM'de bulunur", e);
        }
    }

    /** Yeni bir canlı API key üretir (müşteriye sadece bir kez gösterilir). */
    public static String newApiKey() {
        byte[] bytes = new byte[24];
        RANDOM.nextBytes(bytes);
        return "pl_live_" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
