package io.paramix.redislab.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Redis'in resmi cluster spesifikasyonundaki kurallar; değerler gerçek bir cluster'da CLUSTER KEYSLOT ile doğrulandı. */
class KeySlotTest {

    @Test
    void matchesRedisCrc16() {
        assertThat(KeySlot.crc16("123456789".getBytes())).isEqualTo(0x31C3);   // spesifikasyondaki test vektörü
        assertThat(KeySlot.of("foo")).isEqualTo(12182);
        assertThat(KeySlot.of("123456789")).isEqualTo(12739);
    }

    @Test
    void hashTagPutsRelatedKeysInTheSameSlot() {
        assertThat(KeySlot.of("{user1000}.following")).isEqualTo(KeySlot.of("{user1000}.followers"));
        assertThat(KeySlot.of("cart:{user42}")).isEqualTo(KeySlot.of("orders:{user42}"));
        assertThat(KeySlot.of("cart:user42")).isNotEqualTo(KeySlot.of("orders:user42"));
    }

    @Test
    void hashTagEdgeCasesFromTheSpec() {
        assertThat(KeySlot.hashTagOf("foo{bar}{zap}")).isEqualTo("bar");       // sadece ilk {...}
        assertThat(KeySlot.hashTagOf("foo{}{bar}")).isEqualTo("foo{}{bar}");   // boş tag → bütün key hash'lenir
        assertThat(KeySlot.hashTagOf("foo{{bar}}zap")).isEqualTo("{bar");      // ilk '{' ile ilk '}' arası
        assertThat(KeySlot.hashTagOf("no-tag")).isEqualTo("no-tag");
    }
}
