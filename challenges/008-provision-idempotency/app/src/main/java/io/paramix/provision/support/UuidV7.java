package io.paramix.provision.support;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * RFC 9562 UUIDv7: ilk 48 bit Unix milisaniye, kalanı rastgele.
 *
 * <p>İki yerde işe yarıyor:
 * <ul>
 *   <li><b>Sunucunun ürettiği id'ler</b> (provizyon, çekim, journal): zamana göre artan id, B-tree'de hep sağ uca
 *       yazar. UUIDv4 indeksin her yerine dağılıp sayfa bölünmesine ve cache kaybına yol açar.</li>
 *   <li><b>İstemcinin gönderdiği Idempotency-Key</b>: key kendi doğum zamanını taşıdığı için sunucu
 *       (1) çok eski key'i reddedebilir, (2) key'i hangi günlük partition'da arayacağını bilir.
 *       Bkz. {@code IdempotencyKeyPolicy}.</li>
 * </ul>
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    public static UUID generate(Clock clock) {
        return generate(clock.instant());
    }

    public static UUID generate(Instant at) {
        long ms = at.toEpochMilli();
        byte[] r = new byte[10];
        RANDOM.nextBytes(r);
        long msb = (ms << 16)
                | 0x7000L                                   // version 7
                | ((r[0] & 0x0FL) << 8) | (r[1] & 0xFFL);   // rand_a (12 bit)
        long lsb = 0x8000_0000_0000_0000L                   // variant 10
                | ((r[2] & 0x3FL) << 56);
        for (int i = 3; i < 10; i++) {
            lsb |= (r[i] & 0xFFL) << (8 * (9 - i));
        }
        return new UUID(msb, lsb);
    }

    public static boolean isV7(UUID uuid) {
        return uuid.version() == 7 && uuid.variant() == 2;
    }

    public static Instant timestampOf(UUID uuid) {
        return Instant.ofEpochMilli(uuid.getMostSignificantBits() >>> 16);
    }
}
