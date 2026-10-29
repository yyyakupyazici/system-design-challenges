package io.paramix.notify.store;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Kampanya durumu. Router ve worker her pazarlama mesajında sorar; DB'yi korumak için
 * kısa ömürlü (varsayılan 2 sn) bellek cache'i. "Durdur" en geç bu süre içinde tüm pod'lara yansır
 * (zorluk #7). Daha hızlısı gerekirse Redis Pub/Sub ile anında invalidate edilebilir.
 */
@Component
public class CampaignStore {

    private record Cached(boolean stopped, Instant at) {
    }

    private final JdbcClient jdbc;
    private final Clock clock;
    private final Duration ttl;
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    public CampaignStore(JdbcClient jdbc, Clock clock,
                         @Value("${notify.campaigns.cache-ttl:2s}") Duration ttl) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.ttl = ttl;
    }

    public boolean exists(String campaignId) {
        return jdbc.sql("SELECT count(*) FROM campaign WHERE campaign_id = :id")
                .param("id", campaignId).query(Integer.class).single() > 0;
    }

    /** Bilinmeyen kampanya durdurulmuş sayılır (güvenli taraf). */
    public boolean isStopped(String campaignId) {
        if (campaignId == null) {
            return false;
        }
        Instant now = clock.instant();
        Cached c = cache.get(campaignId);
        if (c != null && c.at().plus(ttl).isAfter(now)) {
            return c.stopped();
        }
        boolean stopped = jdbc.sql("SELECT status FROM campaign WHERE campaign_id = :id")
                .param("id", campaignId).query(String.class).optional()
                .map("STOPPED"::equals).orElse(true);
        cache.put(campaignId, new Cached(stopped, now));
        return stopped;
    }

    public void create(String campaignId, String name) {
        jdbc.sql("""
                        INSERT INTO campaign (campaign_id, name, status) VALUES (:id, :name, 'RUNNING')
                        ON CONFLICT (campaign_id) DO NOTHING""")
                .param("id", campaignId).param("name", name).update();
        cache.remove(campaignId);
    }

    public boolean stop(String campaignId) {
        int n = jdbc.sql("UPDATE campaign SET status = 'STOPPED', updated_at = now() WHERE campaign_id = :id")
                .param("id", campaignId).update();
        cache.remove(campaignId);
        return n == 1;
    }
}
