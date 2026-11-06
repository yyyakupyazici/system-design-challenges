package io.paramix.redislab.hotkey;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Sıcak key (hot key): bir key'e saniyede yüz binlerce istek gelir. Redis Cluster'da bir key tek bir slotta,
 * yani tek bir düğümdedir; o düğümün tek thread'i tıkanır, diğer düğümler boş durur. Cluster'a düğüm eklemek
 * hiçbir şeyi değiştirmez (006'daki "merchant satırı" probleminin Redis versiyonu).
 *
 * <p>Yazma tarafı çözümü: key'i N parçaya böl, her artışı rastgele bir parçaya yaz. Parçaların hash tag'leri
 * FARKLI ({@code {name#0}}, {@code {name#1}}...) → farklı slotlara, farklı düğümlere dağılırlar.
 * Okuma: N parçanın toplamı (N GET). Bedel: okuma pahalılaşır ve toplam atomik değildir (anlık görüntü değil).
 * Sayaç, beğeni sayısı, görüntülenme gibi "yaklaşık anlık" değerler için uygun; bakiye için değil.
 */
@Service
public class ShardedCounter {

    private final StringRedisTemplate redis;
    private final int shards;

    public ShardedCounter(StringRedisTemplate redis, @Value("${lab.hotkey.shards:8}") int shards) {
        this.redis = redis;
        this.shards = shards;
    }

    public List<String> shardKeys(String name) {
        return IntStream.range(0, shards).mapToObj(i -> "counter:{" + name + "#" + i + "}").toList();
    }

    public void increment(String name) {
        int shard = ThreadLocalRandom.current().nextInt(shards);
        redis.opsForValue().increment("counter:{" + name + "#" + shard + "}");
    }

    public long total(String name) {
        // multiGet farklı slotlardaki key'leri cluster'da düğüm düğüm toplar (atomik değil, tek bir ağ turu da değil)
        List<String> values = redis.opsForValue().multiGet(shardKeys(name));
        long sum = 0;
        if (values != null) {
            for (String v : values) {
                sum += v == null ? 0 : Long.parseLong(v);
            }
        }
        return sum;
    }
}
