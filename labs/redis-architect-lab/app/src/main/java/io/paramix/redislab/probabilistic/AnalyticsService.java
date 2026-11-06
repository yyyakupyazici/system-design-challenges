package io.paramix.redislab.probabilistic;

import java.util.List;

import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

/**
 * Az bellekle büyük sorular: Redis'in "özel amaçlı" veri yapıları.
 *
 * <table>
 *   <tr><th>Soru</th><th>Yapı</th><th>Bellek</th><th>Bedel</th></tr>
 *   <tr><td>Bugün kaç TEKİL ziyaretçi?</td><td>HyperLogLog</td><td>key başına en fazla 12 KB, 1 milyar tekil için bile</td>
 *       <td>~%0,81 hata; "Ali geldi mi?" sorulamaz</td></tr>
 *   <tr><td>Kullanıcı X bugün aktif miydi? Kaç kişi aktif?</td><td>Bitmap</td><td>kullanıcı başına 1 bit (10 milyon = 1,2 MB)</td>
 *       <td>Kullanıcı id'leri sayısal ve yoğun olmalı</td></tr>
 *   <tr><td>Bana 2 km içindeki kuryeler?</td><td>Geo (ZSET + geohash)</td><td>eleman başına ~ZSET kadar</td>
 *       <td>Sadece nokta + yarıçap/kutu; poligon yok</td></tr>
 * </table>
 *
 * Cluster notu: birden çok key'i birleştiren komutlar (PFMERGE, BITOP) key'lerin AYNI SLOTTA olmasını ister.
 * Bu yüzden günlük key'ler {@code {sayfa}} / {@code {uygulama}} hash tag'ini paylaşır.
 */
@Service
public class AnalyticsService {

    private final StringRedisTemplate redis;

    public AnalyticsService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    // --- HyperLogLog: tekil sayım ---

    public void visit(String page, String day, String visitorId) {
        redis.opsForHyperLogLog().add("uv:{" + page + "}:" + day, visitorId);
    }

    public long uniqueVisitors(String page, String day) {
        return redis.opsForHyperLogLog().size("uv:{" + page + "}:" + day);
    }

    /** Haftalık tekil = günlük HLL'lerin birleşimi (aynı kişiyi iki kez saymaz). */
    public long uniqueVisitorsOver(String page, List<String> days) {
        String dest = "uv:{" + page + "}:merged";
        String[] sources = days.stream().map(d -> "uv:{" + page + "}:" + d).toArray(String[]::new);
        redis.opsForHyperLogLog().union(dest, sources);
        long n = redis.opsForHyperLogLog().size(dest);
        redis.delete(dest);
        return n;
    }

    // --- Bitmap: günlük aktif kullanıcı ---

    public void markActive(String app, String day, long userId) {
        redis.opsForValue().setBit("dau:{" + app + "}:" + day, userId, true);
    }

    public boolean wasActive(String app, String day, long userId) {
        return Boolean.TRUE.equals(redis.opsForValue().getBit("dau:{" + app + "}:" + day, userId));
    }

    public long activeCount(String app, String day) {
        Long n = redis.execute((RedisCallback<Long>) c -> c.stringCommands().bitCount(("dau:{" + app + "}:" + day).getBytes()));
        return n == null ? 0 : n;
    }

    /** Elde tutma (retention): iki gün de aktif olanlar = BITOP AND. */
    public long activeOnBoth(String app, String day1, String day2) {
        byte[] dest = ("dau:{" + app + "}:and").getBytes();
        Long n = redis.execute((RedisCallback<Long>) c -> {
            c.stringCommands().bitOp(BitOperation.AND, dest, ("dau:{" + app + "}:" + day1).getBytes(),
                    ("dau:{" + app + "}:" + day2).getBytes());
            Long count = c.stringCommands().bitCount(dest);
            c.keyCommands().del(dest);
            return count;
        });
        return n == null ? 0 : n;
    }

    // --- Geo: yakındaki kuryeler ---

    public void updateCourier(String city, String courierId, double lon, double lat) {
        redis.opsForGeo().add("couriers:{" + city + "}", new Point(lon, lat), courierId);
    }

    public List<String> nearestCouriers(String city, double lon, double lat, double km, int limit) {
        GeoResults<GeoLocation<String>> r = redis.opsForGeo().search("couriers:{" + city + "}",
                GeoReference.fromCoordinate(lon, lat), new Distance(km, Metrics.KILOMETERS),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().sortAscending().limit(limit).includeDistance());
        return r == null ? List.of() : r.getContent().stream().map(GeoResult::getContent).map(GeoLocation::getName).toList();
    }
}
