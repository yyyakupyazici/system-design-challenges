package io.paramix.notify.quota;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import io.paramix.notify.config.ProviderProperties;
import io.paramix.notify.config.ProviderProperties.Share;
import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.Priority;

import org.junit.jupiter.api.Test;

class ProviderPropertiesTest {

    @Test
    void validConfig_computesReservedSum() {
        ProviderProperties p = sms(1900, new Share(400, 1400), new Share(500, 1500), new Share(0, 1000));
        assertThat(p.reservedSum()).isEqualTo(900);
    }

    @Test
    void reservedSumAboveGlobal_isRejected() {
        assertThatThrownBy(() -> sms(1000, new Share(600, 600), new Share(500, 500), new Share(0, 0)))
                .hasMessageContaining("garantilerin toplamı");
    }

    /**
     * Chat'teki ilk tablodaki gibi critical tavanı 1.900 olsaydı: critical paylaşılan havuzun tamamını
     * + kendi garantisini isteyebilirdi, ama havuz sadece 1.000. Kural bunu yakalar.
     */
    @Test
    void ceilingAboveReservedPlusSharedPool_isRejected() {
        assertThatThrownBy(() -> sms(1900, new Share(400, 1900), new Share(500, 1500), new Share(0, 1000)))
                .hasMessageContaining("CRITICAL")
                .hasMessageContaining("paylaşılan havuz");
    }

    @Test
    void ceilingBelowReserved_isRejected() {
        assertThatThrownBy(() -> sms(1900, new Share(400, 300), new Share(500, 1500), new Share(0, 1000)))
                .hasMessageContaining("reserved ≤ ceiling");
    }

    private static ProviderProperties sms(int global, Share critical, Share transactional, Share marketing) {
        return new ProviderProperties(Channel.SMS, 1, global,
                Map.of(Priority.CRITICAL, critical, Priority.TRANSACTIONAL, transactional, Priority.MARKETING, marketing),
                true);
    }
}
