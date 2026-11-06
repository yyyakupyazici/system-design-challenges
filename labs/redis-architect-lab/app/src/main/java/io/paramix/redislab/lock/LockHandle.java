package io.paramix.redislab.lock;

/**
 * @param token        bu sahibin rastgele kimliği; kilidi sadece bu token ile bırakabilir/uzatabilirsin
 * @param fencingToken her yeni sahibe artan bir sayı; depolama eski numaralı yazmaları reddeder
 */
public record LockHandle(String resource, String token, long fencingToken) {
}
