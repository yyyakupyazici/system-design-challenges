package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;

import io.paramix.provision.idempotency.PartitionMaintenance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@DisplayName("Idempotency partition bakımı")
class PartitionIT extends IntegrationTest {

    @Autowired
    PartitionMaintenance maintenance;

    @Test
    @DisplayName("Partition'lar önceden açılır, tekrar çağrı zararsız, eski günler DROP edilir")
    void partitionsAreCreatedAheadAndDroppedAfterRetention() {
        LocalDate from = LocalDate.of(2020, 1, 1);
        assertThat(maintenance.ensure(from, from.plusDays(2))).isEqualTo(3);
        assertThat(maintenance.ensure(from, from.plusDays(2))).as("idempotent").isZero();

        assertThat(maintenance.dropBefore(from.plusDays(2))).isEqualTo(2);
        assertThat(partitionExists("idempotency_keys_20200102")).isFalse();
        assertThat(partitionExists("idempotency_keys_20200103")).isTrue();
        maintenance.dropBefore(from.plusDays(3));
    }

    @Test
    @DisplayName("Bugün ve önümüzdeki günlerin partition'ı her zaman hazır")
    void todayAndUpcomingPartitionsExist() {
        maintenance.maintain();
        LocalDate today = LocalDate.now(java.time.ZoneOffset.UTC);
        for (int i = 0; i <= 3; i++) {
            assertThat(partitionExists("idempotency_keys_" + today.plusDays(i).toString().replace("-", ""))).isTrue();
        }
    }

    private boolean partitionExists(String name) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, name));
    }
}
