package io.paylink.gateway.policy;

/** API key rehberi kaydı: "bu key kimin, hangi planda?" */
public record ApiKeyRecord(String keyHash, String tenantId, String plan, boolean active) {
}
