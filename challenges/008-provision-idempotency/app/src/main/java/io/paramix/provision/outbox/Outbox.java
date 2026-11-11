package io.paramix.provision.outbox;

import java.time.Clock;
import java.util.UUID;

import io.paramix.provision.support.UuidV7;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Olayı iş verisiyle AYNI transaction'da yazar. "Bakiyeyi güncelledim ama olayı yayınlayamadım" ya da tersi
 * mümkün değil: ya ikisi birden commit olur ya hiçbiri.
 */
@Component
public class Outbox {

    private final JdbcTemplate jdbc;
    private final ObjectMapper json;
    private final Clock clock;

    public Outbox(JdbcTemplate jdbc, ObjectMapper json, Clock clock) {
        this.jdbc = jdbc;
        this.json = json;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String aggregateType, UUID aggregateId, String eventType, Object payload) {
        try {
            jdbc.update("""
                            INSERT INTO outbox (event_id, aggregate_type, aggregate_id, event_type, payload)
                            VALUES (?, ?, ?, ?, ?::jsonb)
                            """, UuidV7.generate(clock), aggregateType, aggregateId, eventType,
                    json.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Olay serileştirilemedi", e);
        }
    }
}
