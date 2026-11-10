package io.paramix.chat.outbox;

import io.paramix.chat.protocol.Json;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Olayı ÇAĞIRANIN transaction'ı içinde yazar: mesaj ile olayı birlikte yazılır ya da hiçbiri (dual write yok). */
@Component
public class OutboxWriter {

    private final JdbcClient jdbc;
    private final Json json;

    public OutboxWriter(JdbcClient jdbc, Json json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void write(ChatEvent e) {
        jdbc.sql("INSERT INTO outbox (conversation_id, payload) VALUES (?, ?)")
                .params(e.conversationId(), json.write(e)).update();
    }
}
