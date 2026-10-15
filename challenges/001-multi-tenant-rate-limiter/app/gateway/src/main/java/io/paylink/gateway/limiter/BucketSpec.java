package io.paylink.gateway.limiter;

/**
 * Bir token bucket: Redis key'i + kuraldan gelen rate/burst.
 * rate/burst Redis'te saklanmaz, her çağrıda Lua'ya parametre olarak gider.
 */
public record BucketSpec(String redisKey, String layer, int ratePerSec, int burst) {
}
