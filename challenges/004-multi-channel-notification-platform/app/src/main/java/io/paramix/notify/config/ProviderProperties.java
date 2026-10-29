package io.paramix.notify.config;

import java.util.EnumMap;
import java.util.Map;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.Priority;

import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Bir sağlayıcının kanalı, sırası (1 = birincil) ve öncelik bazında kotası.
 *
 * <p>Kapasite modeli: her önceliğin garantisi + paylaşılan havuz (global − Σ garanti).
 * Başlangıçta doğrulanır, yanlış config ile uygulama ayağa kalkmaz.
 */
public record ProviderProperties(
        Channel channel,
        @DefaultValue("1") int order,
        int globalRate,
        Map<Priority, Share> priorities,
        @DefaultValue("true") boolean dedupByClientRef) {

    public record Share(int reserved, int ceiling) {
    }

    public ProviderProperties {
        if (channel == null) {
            throw new IllegalArgumentException("provider channel zorunlu");
        }
        if (globalRate <= 0) {
            throw new IllegalArgumentException("globalRate > 0 olmalı");
        }
        Map<Priority, Share> p = new EnumMap<>(Priority.class);
        if (priorities != null) {
            p.putAll(priorities);
        }
        for (Priority pr : Priority.values()) {
            p.putIfAbsent(pr, new Share(0, 0));
        }
        priorities = Map.copyOf(p);

        int reservedSum = priorities.values().stream().mapToInt(Share::reserved).sum();
        if (reservedSum > globalRate) {
            throw new IllegalArgumentException(
                    "garantilerin toplamı (" + reservedSum + ") globalRate'i (" + globalRate + ") aşıyor");
        }
        int shared = globalRate - reservedSum;
        for (Map.Entry<Priority, Share> e : priorities.entrySet()) {
            Share s = e.getValue();
            if (s.reserved() < 0 || s.ceiling() < s.reserved()) {
                throw new IllegalArgumentException(e.getKey() + ": 0 ≤ reserved ≤ ceiling olmalı");
            }
            if (s.ceiling() > s.reserved() + shared) {
                throw new IllegalArgumentException(e.getKey() + ": ceiling (" + s.ceiling()
                        + ") > reserved + paylaşılan havuz (" + (s.reserved() + shared) + ")");
            }
        }
    }

    public int reservedSum() {
        return priorities.values().stream().mapToInt(Share::reserved).sum();
    }

    public Share share(Priority priority) {
        return priorities.get(priority);
    }
}
