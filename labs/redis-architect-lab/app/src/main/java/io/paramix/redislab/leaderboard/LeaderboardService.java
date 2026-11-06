package io.paramix.redislab.leaderboard;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;

/**
 * Liderlik tablosu = ZSET'in ders kitabı örneği. Veritabanında "ilk 10" ve "ben kaçıncıyım?" her seferinde
 * bütün tabloyu sıralamak demektir ({@code ORDER BY score DESC} + {@code COUNT(*) WHERE score > ?}).
 * ZSET'te ikisi de O(log N): 10 milyon oyuncuda bile mikrosaniyeler.
 */
@Service
public class LeaderboardService {

    public record Entry(String member, long score, long rank) {
    }

    private final StringRedisTemplate redis;

    public LeaderboardService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    static String key(String board) {
        return "lb:{" + board + "}";
    }

    /** ZINCRBY atomik: aynı oyuncuya iki sunucudan aynı anda puan eklense de hiçbir artış kaybolmaz. */
    public long addScore(String board, String member, long delta) {
        Double s = redis.opsForZSet().incrementScore(key(board), member, delta);
        return s == null ? 0 : s.longValue();
    }

    public List<Entry> top(String board, int n) {
        return toEntries(redis.opsForZSet().reverseRangeWithScores(key(board), 0, n - 1), 1);
    }

    /** 1'den başlayan sıra; tabloda yoksa -1. */
    public long rank(String board, String member) {
        Long r = redis.opsForZSet().reverseRank(key(board), member);
        return r == null ? -1 : r + 1;
    }

    /** "Ben ve etrafımdakiler": oyunlarda en çok kullanılan görünüm. */
    public List<Entry> around(String board, String member, int k) {
        Long r = redis.opsForZSet().reverseRank(key(board), member);
        if (r == null) {
            return List.of();
        }
        long from = Math.max(0, r - k);
        return toEntries(redis.opsForZSet().reverseRangeWithScores(key(board), from, r + k), from + 1);
    }

    private static List<Entry> toEntries(Set<TypedTuple<String>> tuples, long firstRank) {
        List<Entry> out = new ArrayList<>();
        long rank = firstRank;
        if (tuples != null) {
            for (TypedTuple<String> t : tuples) {
                out.add(new Entry(t.getValue(), t.getScore() == null ? 0 : t.getScore().longValue(), rank++));
            }
        }
        return out;
    }
}
