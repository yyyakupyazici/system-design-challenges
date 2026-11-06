package io.paramix.redislab.ratelimit;

/** @param count penceredeki istek sayısı ya da kalan jeton (algoritmaya göre) */
public record Decision(boolean allowed, long count) {
}
