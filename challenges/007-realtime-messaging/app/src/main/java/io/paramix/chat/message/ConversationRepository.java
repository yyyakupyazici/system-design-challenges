package io.paramix.chat.message;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.paramix.chat.config.ChatProperties;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Sohbet ve üyelik okumaları. Üye listesi fan-out'ta her mesajda gerekir; kısa süre bellekte tutulur
 * (500 kişilik bir gruba saniyede 10 mesaj gelirse DB'ye saniyede 10 kez 500 satır sorgusu gitmesin).
 */
@Repository
public class ConversationRepository {

    public record Conversation(long id, String kind, long lastSeq) {
    }

    public record Membership(String role, long readSeq) {
    }

    private record Cached(List<Long> members, long expiresAt) {
    }

    private final JdbcClient jdbc;
    private final Duration ttl;
    private final Map<Long, Cached> membersCache = new ConcurrentHashMap<>();

    public ConversationRepository(JdbcClient jdbc, ChatProperties props) {
        this.jdbc = jdbc;
        this.ttl = props.membersCacheTtl();
    }

    public Optional<Conversation> find(long id) {
        return jdbc.sql("SELECT id, kind, last_seq FROM conversations WHERE id = ?").param(id)
                .query((rs, n) -> new Conversation(rs.getLong(1), rs.getString(2), rs.getLong(3))).optional();
    }

    public Optional<Membership> membership(long conversationId, long userId) {
        return jdbc.sql("SELECT role, read_seq FROM conversation_members WHERE conversation_id = ? AND user_id = ?")
                .params(conversationId, userId)
                .query((rs, n) -> new Membership(rs.getString(1), rs.getLong(2))).optional();
    }

    public List<Long> members(long conversationId) {
        long now = System.currentTimeMillis();
        Cached c = membersCache.get(conversationId);
        if (c != null && c.expiresAt() > now) {
            return c.members();
        }
        List<Long> m = jdbc.sql("SELECT user_id FROM conversation_members WHERE conversation_id = ?")
                .param(conversationId).query(Long.class).list();
        membersCache.put(conversationId, new Cached(m, now + ttl.toMillis()));
        return m;
    }

    /** Kullanıcının üye olduğu sohbetler ve son seq'leri (senkron için). */
    public List<Conversation> conversationsOf(long userId) {
        return jdbc.sql("""
                        SELECT c.id, c.kind, c.last_seq FROM conversations c
                        JOIN conversation_members m ON m.conversation_id = c.id
                        WHERE m.user_id = ?""")
                .param(userId)
                .query((rs, n) -> new Conversation(rs.getLong(1), rs.getString(2), rs.getLong(3))).list();
    }

    public List<Long> broadcastChannelsOf(long userId) {
        return jdbc.sql("""
                        SELECT c.id FROM conversations c JOIN conversation_members m ON m.conversation_id = c.id
                        WHERE m.user_id = ? AND c.kind = 'BROADCAST'""")
                .param(userId).query(Long.class).list();
    }

    public boolean presenceHidden(long userId) {
        return jdbc.sql("SELECT presence_hidden FROM users WHERE id = ?").param(userId)
                .query(Boolean.class).optional().orElse(false);
    }
}
