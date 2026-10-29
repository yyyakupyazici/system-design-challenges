package io.paramix.notify.store;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.paramix.notify.domain.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Şablonlar az ve nadiren değişir: tamamı pod belleğinde tutulur, periyodik yenilenir.
 * Router bildirim başına DB'ye gitmez.
 */
@Component
public class TemplateStore {

    private static final Logger log = LoggerFactory.getLogger(TemplateStore.class);
    private static final String DEFAULT_LOCALE = "tr";

    public record Template(String templateId, String locale, Channel channel, int version, String title, String body) {
    }

    private final JdbcClient jdbc;
    private final AtomicReference<Map<String, Template>> cache = new AtomicReference<>(Map.of());

    public TemplateStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Önce kullanıcının dili, yoksa varsayılan dil. */
    public Optional<Template> find(String templateId, String locale, Channel channel) {
        Map<String, Template> m = cache.get();
        if (m.isEmpty()) {
            refresh();
            m = cache.get();
        }
        Template t = m.get(key(templateId, locale, channel));
        if (t == null) {
            t = m.get(key(templateId, DEFAULT_LOCALE, channel));
        }
        return Optional.ofNullable(t);
    }

    @Scheduled(fixedDelayString = "${notify.templates.refresh-ms:60000}", initialDelay = 0)
    public void refresh() {
        try {
            Map<String, Template> loaded = jdbc.sql("SELECT template_id, locale, channel, version, title, body FROM template")
                    .query((rs, i) -> new Template(rs.getString("template_id"), rs.getString("locale"),
                            Channel.valueOf(rs.getString("channel")), rs.getInt("version"),
                            rs.getString("title"), rs.getString("body")))
                    .list().stream()
                    .collect(Collectors.toUnmodifiableMap(t -> key(t.templateId(), t.locale(), t.channel()), t -> t));
            cache.set(loaded);
        } catch (RuntimeException e) {
            log.warn("Şablonlar yenilenemedi, eski kopya kullanılıyor: {}", e.getMessage());
        }
    }

    private static String key(String templateId, String locale, Channel channel) {
        return templateId + "|" + locale + "|" + channel;
    }
}
