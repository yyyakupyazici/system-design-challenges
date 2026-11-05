package io.paramix.llmgw.budget;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.TeamProperties;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Bütçe = rezervasyon + mutabakat. İstek başında en fazla tutar bloke edilir; stream bitince gerçek tutar
 * yazılır, fark iade edilir. Böylece stream ortasında bütçe bitmez: yetmiyorsa istek hiç başlamaz.
 *
 * <p>Redis hızlı karar içindir; gerçeğin kaynağı kullanım defteri (PostgreSQL). Gece bir iş defteri sağlayıcı
 * faturasıyla karşılaştırır (README).
 */
@Service
public class BudgetService {

    private static final Logger log = LoggerFactory.getLogger(BudgetService.class);
    private static final long MICRO = 1_000_000L;

    public enum Denial { USER_CEILING, TEAM_POOL }

    public record Outcome(Reservation reservation, Denial denial) {
        public boolean granted() {
            return reservation != null;
        }
    }

    private final StringRedisTemplate redis;
    private final GatewayProperties props;
    private final Clock clock;
    private final MeterRegistry meters;
    private final DefaultRedisScript<List> reserve = script("scripts/budget_reserve.lua");
    private final DefaultRedisScript<List> settle = script("scripts/budget_settle.lua");

    public BudgetService(StringRedisTemplate redis, GatewayProperties props, Clock clock, MeterRegistry meters) {
        this.redis = redis;
        this.props = props;
        this.clock = clock;
        this.meters = meters;
    }

    public Outcome reserve(String team, String userId, String requestId, long amountMicro) {
        TeamProperties t = team(team);
        String day = today();
        boolean member = t.members().contains(userId);
        long guarantee = member ? t.guaranteePerMemberCredits() * MICRO : 0;
        long expiresAt = clock.millis() + props.limits().reservationTtl().toMillis();
        List<?> r = redis.execute(reserve, keys(team, day, userId, requestId),
                String.valueOf(amountMicro), String.valueOf(guarantee), String.valueOf(t.poolCredits() * MICRO),
                String.valueOf(t.userCeilingCredits() * MICRO), String.valueOf(expiresAt),
                String.valueOf(Duration.ofDays(3).toSeconds()), userId, requestId);
        if (((Number) r.get(0)).longValue() == 1) {
            return new Outcome(new Reservation(requestId, team, day, userId, amountMicro), null);
        }
        Denial d = ((Number) r.get(1)).longValue() == 1 ? Denial.USER_CEILING : Denial.TEAM_POOL;
        meters.counter("llmgw.budget.denied", "team", team, "reason", d.name()).increment();
        return new Outcome(null, d);
    }

    /** @param actualMicro gerçek maliyet; -1 = bilinmiyor (rezerve tutar harcanmış sayılır) */
    public void settle(Reservation res, long actualMicro) {
        List<?> r = redis.execute(settle, keys(res.team(), res.day(), res.userId(), res.id()),
                String.valueOf(actualMicro), res.id());
        if (((Number) r.get(0)).longValue() == 1) {
            checkAlerts(res.team(), res.day(), ((Number) r.get(1)).longValue());
        }
    }

    /** Ekibin bugünkü durumu (rapor / debug). */
    public Map<String, Object> status(String team, String userId) {
        String day = today();
        Map<Object, Object> t = redis.opsForHash().entries(prefix(team, day));
        Map<Object, Object> u = redis.opsForHash().entries(prefix(team, day) + ":u:" + userId);
        TeamProperties tp = team(team);
        return Map.of(
                "team", team, "day", day,
                "dailyCredits", tp.dailyCredits(),
                "spentCredits", credits(t.get("spent")),
                "reservedCredits", credits(t.get("reserved")),
                "poolUsedCredits", credits(t.get("pool")),
                "poolCredits", tp.poolCredits(),
                "user", Map.of("userId", userId,
                        "guaranteeUsedCredits", credits(u.get("own")),
                        "poolUsedCredits", credits(u.get("pool")),
                        "spentCredits", credits(u.get("spent"))));
    }

    /**
     * Pod stream ortasında öldüyse settle hiç gelmez. Süresi geçen rezervasyonlar temkinli kapatılır:
     * rezerve tutarın tamamı harcanmış sayılır (gerçek tüketim, sağlayıcının kullanım raporuyla sonra düzeltilir).
     */
    @Scheduled(fixedDelayString = "${gateway.limits.reaper-ms:30000}")
    public void reapExpired() {
        long now = clock.millis();
        for (String team : props.teams().keySet()) {
            for (String day : Set.of(today(), LocalDate.now(clock.withZone(zone())).minusDays(1).toString())) {
                String zkey = prefix(team, day) + ":rz";
                Set<String> expired = redis.opsForZSet().rangeByScore(zkey, 0, now);
                if (expired == null) {
                    continue;
                }
                for (String id : expired) {
                    Object user = redis.opsForHash().get(prefix(team, day) + ":r:" + id, "user");
                    if (user == null) {
                        redis.opsForZSet().remove(zkey, id);
                        continue;
                    }
                    settle(new Reservation(id, team, day, user.toString(), -1), -1);
                    meters.counter("llmgw.budget.reaped", "team", team).increment();
                    log.warn("Süresi geçen rezervasyon kapatıldı (rezerve tutar harcanmış sayıldı): {} {}", team, id);
                }
            }
        }
    }

    /** %80 ve %100 eşikleri günde bir kez bildirilir (gerçek sistemde 004'teki bildirim platformuna gider). */
    private void checkAlerts(String team, String day, long spentMicro) {
        long limit = team(team).dailyCredits() * MICRO;
        for (int pct : new int[] {80, 100}) {
            if (spentMicro * 100 >= limit * pct) {
                Boolean first = redis.opsForValue().setIfAbsent(prefix(team, day) + ":alert" + pct, "1", Duration.ofDays(2));
                if (Boolean.TRUE.equals(first)) {
                    meters.counter("llmgw.budget.alert", "team", team, "threshold", String.valueOf(pct)).increment();
                    log.warn("BÜTÇE UYARISI: {} ekibi günlük bütçesinin %{}'ini kullandı", team, pct);
                }
            }
        }
    }

    public Optional<TeamProperties> teamProps(String team) {
        return Optional.ofNullable(props.teams().get(team));
    }

    private TeamProperties team(String team) {
        TeamProperties t = props.teams().get(team);
        if (t == null) {
            throw new IllegalArgumentException("Bilinmeyen ekip: " + team);
        }
        return t;
    }

    private String today() {
        return LocalDate.now(clock.withZone(zone())).toString();
    }

    private ZoneId zone() {
        return ZoneId.of(props.limits().budgetTimeZone());
    }

    static String prefix(String team, String day) {
        return "b:{" + team + ":" + day + "}";
    }

    private static List<String> keys(String team, String day, String user, String resId) {
        String p = prefix(team, day);
        return List.of(p, p + ":u:" + user, p + ":r:" + resId, p + ":rz");
    }

    private static double credits(Object micro) {
        return micro == null ? 0.0 : Long.parseLong(micro.toString()) / (double) MICRO;
    }

    private static DefaultRedisScript<List> script(String path) {
        DefaultRedisScript<List> s = new DefaultRedisScript<>();
        s.setLocation(new ClassPathResource(path));
        s.setResultType(List.class);
        return s;
    }
}
