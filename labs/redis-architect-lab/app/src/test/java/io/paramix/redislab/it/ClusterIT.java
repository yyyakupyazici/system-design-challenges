package io.paramix.redislab.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import io.paramix.redislab.cluster.ClusterInspector;
import io.paramix.redislab.common.KeySlot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;

/** Sadece gerçek bir Redis Cluster'a karşı çalışır (IT_REDIS_CLUSTER). */
@EnabledIf("io.paramix.redislab.it.TestRedis#isCluster")
class ClusterIT extends AbstractRedisIT {

    @Autowired ClusterInspector cluster;

    @Test
    void topologyHasThreeMastersCoveringAllSlots() {
        assertThat(cluster.isCluster()).isTrue();
        assertThat(cluster.topology()).filteredOn(n -> n.role().equals("master")).hasSize(3);
    }

    @Test
    void ourSlotCalculationMatchesTheServer() {
        for (String k : List.of("foo", "user:42", "cart:{user42}", "stock:{iphone}:orders", "x{}y")) {
            assertThat(cluster.serverSlot(k)).as(k).isEqualTo(KeySlot.of(k));
        }
    }

    @Test
    void multiKeyScriptFailsAcrossSlotsAndWorksWithAHashTag() {
        String u = unique("u");
        String a = "a:" + u;
        String b = "b:" + u;
        int i = 0;
        while (KeySlot.of(a) == KeySlot.of(b)) {                 // farklı slotta olduklarından emin ol
            b = "b:" + u + ":" + i++;
        }
        String finalB = b;
        assertThatThrownBy(() -> cluster.touchBoth(a, finalB))
                .isInstanceOf(DataAccessException.class)
                .rootCause().hasMessageContaining("CROSSSLOT");

        cluster.touchBoth("a:{" + u + "}", "b:{" + u + "}");      // aynı hash tag → aynı slot → çalışır
    }
}
