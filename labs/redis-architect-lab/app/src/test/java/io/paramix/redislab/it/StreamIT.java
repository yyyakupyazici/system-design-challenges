package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.paramix.redislab.streams.OrderEventStream;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;

class StreamIT extends AbstractRedisIT {

    @Autowired OrderEventStream events;

    @Test
    void unackedMessagesAreRecoveredByAnotherConsumerAndPoisonGoesToDlq() throws Exception {
        String s = unique("orders");
        events.ensureGroup(s, "billing");
        events.ensureGroup(s, "billing");                      // ikinci kez: BUSYGROUP yok sayılır
        for (int i = 1; i <= 5; i++) {
            events.publish(s, Map.of("orderId", "o-" + i, "type", "CREATED"));
        }

        // A beşini alır, üçünü işler, sonra "ölür"
        List<MapRecord<String, Object, Object>> got = events.read(s, "billing", "consumer-a", 10);
        assertThat(got).hasSize(5);
        got.subList(0, 3).forEach(r -> events.ack(s, "billing", r.getId()));
        assertThat(events.pending(s, "billing", 10).size()).isEqualTo(2);

        // B, takılanları devralır (teslim sayısı 2 olur)
        Thread.sleep(100);
        List<MapRecord<String, Object, Object>> recovered = events.recover(s, "billing", "consumer-b", Duration.ofMillis(50), 3);
        assertThat(recovered).extracting(r -> r.getValue().get("orderId")).containsExactly("o-4", "o-5");

        // B de işleyemedi; bir tur daha (teslim sayısı 3) → artık zehirli, DLQ'ya
        Thread.sleep(100);
        events.recover(s, "billing", "consumer-c", Duration.ofMillis(50), 3);
        Thread.sleep(100);
        events.recover(s, "billing", "consumer-d", Duration.ofMillis(50), 3);
        assertThat(events.dlqSize(s)).isEqualTo(2);
        assertThat(events.pending(s, "billing", 10).size()).isZero();
    }

    @Test
    void eachMessageGoesToOneConsumerInAGroupButEveryGroupGetsAll() {
        String s = unique("orders");
        events.ensureGroup(s, "billing");
        events.ensureGroup(s, "notification");
        events.publish(s, Map.of("orderId", "o-1"));
        events.publish(s, Map.of("orderId", "o-2"));

        int a = events.read(s, "billing", "a", 1).size();
        int b = events.read(s, "billing", "b", 10).size();
        assertThat(a + b).isEqualTo(2);                         // grup içinde paylaşıldı
        assertThat(events.read(s, "notification", "n", 10)).hasSize(2);   // diğer grup hepsini aldı
    }
}
