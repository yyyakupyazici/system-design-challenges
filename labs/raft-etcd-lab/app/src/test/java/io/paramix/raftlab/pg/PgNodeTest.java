package io.paramix.raftlab.pg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PgNodeTest {

    @Test
    void lsnIsComparedAsABytePosition() {
        assertThat(PgNode.lsnToBytes("0/3A000128")).isEqualTo(0x3A000128L);
        assertThat(PgNode.lsnToBytes("1/0")).isEqualTo(1L << 32);
        assertThat(PgNode.lsnToBytes("1/0")).as("üst kısım büyük olan ileride").isGreaterThan(PgNode.lsnToBytes("0/FFFFFFFF"));
        assertThat(PgNode.lsnToBytes(null)).isZero();
    }
}
