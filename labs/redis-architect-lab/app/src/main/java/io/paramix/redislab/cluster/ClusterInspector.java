package io.paramix.redislab.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.paramix.redislab.common.KeySlot;
import io.paramix.redislab.common.Scripts;

import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Cluster'ı görünür kılar: hangi düğüm hangi slotlara sahip, bir key hangi slota düşer,
 * ve neden çok-key'li işlemler aynı hash tag'i paylaşmak zorunda.
 */
@Service
public class ClusterInspector {

    public record NodeInfo(String address, String role, String slots) {
    }

    private static final RedisScript<Long> CROSS = Scripts.load("cross_slot_demo", Long.class);

    private final RedisConnectionFactory factory;
    private final StringRedisTemplate redis;

    public ClusterInspector(RedisConnectionFactory factory, StringRedisTemplate redis) {
        this.factory = factory;
        this.redis = redis;
    }

    public boolean isCluster() {
        try (RedisConnection c = factory.getConnection()) {
            return c instanceof RedisClusterConnection;
        }
    }

    public List<NodeInfo> topology() {
        List<NodeInfo> out = new ArrayList<>();
        try (RedisConnection c = factory.getConnection()) {
            if (!(c instanceof RedisClusterConnection cc)) {
                return List.of(new NodeInfo("standalone", "master", "0-16383"));
            }
            for (RedisClusterNode n : cc.clusterGetNodes()) {
                String slots = n.getSlotRange() == null || n.getSlotRange().getSlots().isEmpty() ? "-"
                        : n.getSlotRange().getSlots().stream().mapToInt(Integer::intValue).min().getAsInt() + "-"
                        + n.getSlotRange().getSlots().stream().mapToInt(Integer::intValue).max().getAsInt();
                out.add(new NodeInfo(n.getHost() + ":" + n.getPort(), n.isMaster() ? "master" : "replica", slots));
            }
        }
        return out;
    }

    /** Sunucunun hesapladığı slot (cluster'da CLUSTER KEYSLOT), yoksa yerel hesap. */
    public int serverSlot(String key) {
        try (RedisConnection c = factory.getConnection()) {
            if (c instanceof RedisClusterConnection cc) {
                return cc.clusterGetSlotForKey(key.getBytes());
            }
        }
        return KeySlot.of(key);
    }

    public Map<String, Integer> slots(List<String> keys) {
        Map<String, Integer> m = new java.util.LinkedHashMap<>();
        keys.forEach(k -> m.put(k, KeySlot.of(k)));
        return m;
    }

    /** İki key'e dokunan bir script çalıştırır. Cluster'da key'ler farklı slottaysa CROSSSLOT hatası fırlar. */
    public void touchBoth(String key1, String key2) {
        redis.execute(CROSS, List.of(key1, key2), "x");
    }
}
