package io.paramix.redislab.common;

import java.nio.charset.StandardCharsets;

/**
 * Redis Cluster bir key'i hangi slota koyar? {@code slot = CRC16(key) mod 16384}. Key'de {@code {...}} varsa
 * SADECE süslü parantezin içi hash'lenir (hash tag). Böylece {@code cart:{user42}} ile {@code orders:{user42}}
 * aynı slota, yani aynı düğüme düşer ve tek bir Lua script'i ya da MULTI içinde birlikte kullanılabilir.
 *
 * <p>Sunucuya sormadan hesaplamak, tasarım sırasında "bu iki key aynı slotta mı?" sorusunu test etmeyi sağlar.
 */
public final class KeySlot {

    public static final int SLOTS = 16384;

    private KeySlot() {
    }

    public static int of(String key) {
        return crc16(hashTagOf(key).getBytes(StandardCharsets.UTF_8)) % SLOTS;
    }

    /** Redis kuralı: ilk '{' ile ondan sonraki ilk '}' arası boş değilse hash'lenen kısım odur. */
    public static String hashTagOf(String key) {
        int open = key.indexOf('{');
        if (open >= 0) {
            int close = key.indexOf('}', open + 1);
            if (close > open + 1) {
                return key.substring(open + 1, close);
            }
        }
        return key;
    }

    /** CRC16-CCITT (XModem), Redis'in kullandığı varyant. */
    static int crc16(byte[] bytes) {
        int crc = 0;
        for (byte b : bytes) {
            crc ^= (b & 0xFF) << 8;
            for (int i = 0; i < 8; i++) {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
                crc &= 0xFFFF;
            }
        }
        return crc;
    }
}
