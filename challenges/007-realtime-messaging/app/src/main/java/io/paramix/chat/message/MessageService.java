package io.paramix.chat.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.message.ConversationRepository.Conversation;
import io.paramix.chat.message.ConversationRepository.Membership;
import io.paramix.chat.outbox.ChatEvent;
import io.paramix.chat.outbox.OutboxWriter;
import io.paramix.chat.protocol.Frames;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Mesajın kalıcı yolu. Kural: ÖNCE KAYDET, SONRA TESLİM ET. Gönderen ✓'yı ancak mesaj veritabanına yazılınca alır;
 * teslimat (Kafka → fan-out → Pub/Sub → gateway) arka planda, kaybolsa bile istemcinin senkronu yakalar.
 *
 * <pre>
 *  tek transaction:
 *    ① UPDATE conversations SET last_seq = last_seq + 1     → sohbet başına sıra numarası, satır kilidiyle
 *    ② INSERT messages ... ON CONFLICT (conversation_id, client_msg_id) DO NOTHING
 *         çakışma = aynı mesaj tekrar geldi → ROLLBACK (numara boşa gitmez), mevcut mesajı aynı seq ile döndür
 *    ③ INSERT outbox (olay)                                  → relay Kafka'ya taşıyacak
 * </pre>
 */
@Service
public class MessageService {

    public record SendCommand(UUID clientMsgId, long conversationId, String kind, String body) {
    }

    public record SendResult(MessageView message, boolean duplicate) {
    }

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final ConversationRepository conversations;
    private final MessageRepository messages;
    private final OutboxWriter outbox;
    private final ChatProperties props;
    private final Semaphore syncSlots;
    private final MeterRegistry meters;

    public MessageService(JdbcClient jdbc, TransactionTemplate tx, ConversationRepository conversations,
                          MessageRepository messages, OutboxWriter outbox,
                          ChatProperties props, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.conversations = conversations;
        this.messages = messages;
        this.outbox = outbox;
        this.props = props;
        this.meters = meters;
        this.syncSlots = new Semaphore(props.sync().maxConcurrent());
    }

    // ------------------------------------------------------------------ gönderme

    public SendResult send(long userId, String deviceId, SendCommand cmd) {
        validateInput(cmd);                                // ucuz kontroller önce, veritabanına gitmeden
        Conversation conv = conversations.find(cmd.conversationId())
                .orElseThrow(() -> new ChatException("NOT_FOUND", "sohbet yok"));
        Membership me = conversations.membership(conv.id(), userId)
                .orElseThrow(() -> new ChatException("FORBIDDEN", "bu sohbetin üyesi değilsin"));
        if ("BROADCAST".equals(conv.kind()) && !"ADMIN".equals(me.role())) {
            throw new ChatException("FORBIDDEN", "kampanya kanalına sadece yöneticiler yazar");
        }
        String kind = cmd.kind() == null ? "TEXT" : cmd.kind();
        validate(conv, userId, kind, cmd);

        MessageView saved = tx.execute(status -> {
            long seq = messages.nextSeq(conv.id());
            if (!messages.insert(conv.id(), seq, userId, cmd.clientMsgId(), kind, cmd.body(), null)) {
                status.setRollbackOnly();          // tekrar gelen mesaj: sayaç artışı geri alınır
                return null;
            }
            MessageView v = messages.bySeq(conv.id(), seq).orElseThrow();
            outbox.write(new ChatEvent(conv.id(), conv.kind(), userId, userId + ":" + deviceId,
                    "BROADCAST".equals(conv.kind()) ? null : "Yeni mesaj", Frames.msg(v)));
            return v;
        });
        if (saved != null) {
            meters.counter("chat.messages.saved", "kind", kind).increment();
            return new SendResult(saved, false);
        }
        meters.counter("chat.messages.duplicate").increment();
        return new SendResult(messages.byClientId(conv.id(), cmd.clientMsgId()).orElseThrow(), true);
    }

    private static void validateInput(SendCommand cmd) {
        if (cmd.clientMsgId() == null) {
            throw new ChatException("BAD_REQUEST", "clientMsgId zorunlu (idempotency anahtarı)");
        }
        if (cmd.body() == null || cmd.body().isBlank() || cmd.body().length() > 4000) {
            throw new ChatException("BAD_REQUEST", "mesaj boş ya da 4000 karakterden uzun");
        }
    }

    /**
     * Soketten sadece metin gelir. Para isteği kartı (PAYMENT_REQUEST) ve durum mesajları (SYSTEM) istemciden değil,
     * payment-request-service'in olaylarından yazılır (bkz. paymentcard.PaymentCardsConsumer).
     */
    private static void validate(Conversation conv, long userId, String kind, SendCommand cmd) {
        if (!"TEXT".equals(kind)) {
            throw new ChatException("BAD_REQUEST", "bilinmeyen mesaj türü: " + kind
                    + ("PAYMENT_REQUEST".equals(kind) ? " (para isteği payment-request-service'e gönderilir)" : ""));
        }
    }

    // ------------------------------------------------------------------ iletildi / okundu

    /** ✓✓ (iletildi): cihaz mesajı aldığını onayladı. Birebir sohbette karşı tarafa bildirilir. */
    public void ack(long userId, String deviceId, long conversationId, long seq) {
        Conversation conv = memberConversation(conversationId, userId);
        tx.executeWithoutResult(s -> {
            boolean advanced = advanceDeviceCursor(userId, deviceId, conversationId, seq);
            if (advanced && "DIRECT".equals(conv.kind())) {
                outbox.write(new ChatEvent(conversationId, conv.kind(), userId, userId + ":" + deviceId, null,
                        Frames.receipt(conversationId, userId, seq, "DELIVERED")));
            }
        });
    }

    /**
     * Okundu: kullanıcının okuma imleci ilerler (bütün cihazları için tek imleç). Olay sohbetin bütün üyelerine
     * gider: gönderen mavi ✓✓ görür, okuyanın DİĞER cihazları da "okundu" olarak işaretler (çoklu cihaz senkronu).
     */
    public void read(long userId, String deviceId, long conversationId, long seq) {
        Conversation conv = memberConversation(conversationId, userId);
        if (seq > conv.lastSeq()) {
            throw new ChatException("BAD_REQUEST", "henüz var olmayan mesaj okunamaz");
        }
        tx.executeWithoutResult(s -> {
            advanceDeviceCursor(userId, deviceId, conversationId, seq);
            int n = jdbc.sql("UPDATE conversation_members SET read_seq = ? WHERE conversation_id = ? AND user_id = ? AND read_seq < ?")
                    .params(seq, conversationId, userId, seq).update();
            if (n == 1 && !"BROADCAST".equals(conv.kind())) {
                outbox.write(new ChatEvent(conversationId, conv.kind(), userId, userId + ":" + deviceId, null,
                        Frames.receipt(conversationId, userId, seq, "READ")));
            }
        });
    }

    private boolean advanceDeviceCursor(long userId, String deviceId, long conversationId, long seq) {
        return jdbc.sql("""
                        INSERT INTO device_cursors (user_id, device_id, conversation_id, delivered_seq) VALUES (?, ?, ?, ?)
                        ON CONFLICT (user_id, device_id, conversation_id) DO UPDATE SET delivered_seq = EXCLUDED.delivered_seq
                        WHERE device_cursors.delivered_seq < EXCLUDED.delivered_seq""")
                .params(userId, deviceId, conversationId, seq).update() == 1;
    }

    private Conversation memberConversation(long conversationId, long userId) {
        if (conversations.membership(conversationId, userId).isEmpty()) {
            throw new ChatException("FORBIDDEN", "bu sohbetin üyesi değilsin");
        }
        return conversations.find(conversationId).orElseThrow();
    }

    // ------------------------------------------------------------------ senkron

    /**
     * Çevrimdışı dönüş / kopan bağlantı / kaçırılan mesaj. İstemci elindeki imleçleri gönderir ("sohbet 81'de 41'e
     * kadar gördüm"); yoksa cihazın kayıtlı imleci, o da yoksa (yeni cihaz) son N mesaj.
     *
     * <p>Bulkhead: aynı anda en fazla {@code maxConcurrent} senkron. 50.000 istemci aynı saniyede geri bağlanınca
     * hepsi veritabanına aynı anda yüklenmesin; sığmayan "BUSY" alır ve rastgele beklemeyle tekrar dener.
     */
    public List<Map<String, Object>> sync(long userId, String deviceId, Map<Long, Long> clientCursors) {
        boolean acquired;
        try {
            acquired = syncSlots.tryAcquire(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            acquired = false;
        }
        if (!acquired) {
            meters.counter("chat.sync.rejected").increment();
            throw new ChatException("BUSY", "senkron kuyruğu dolu, biraz bekleyip tekrar dene");
        }
        try {
            Map<Long, Long> stored = new HashMap<>();
            jdbc.sql("SELECT conversation_id, delivered_seq FROM device_cursors WHERE user_id = ? AND device_id = ?")
                    .params(userId, deviceId).query(rs -> {
                        stored.put(rs.getLong(1), rs.getLong(2));
                    });
            List<Map<String, Object>> out = new ArrayList<>();
            int page = props.sync().pageSize();
            for (Conversation c : conversations.conversationsOf(userId)) {
                long cursor = clientCursors.getOrDefault(c.id(),
                        stored.getOrDefault(c.id(), Math.max(0, c.lastSeq() - props.sync().newDeviceHistory())));
                if (c.lastSeq() <= cursor) {
                    continue;
                }
                List<MessageView> ms = messages.after(c.id(), cursor, page);
                long lastReturned = ms.isEmpty() ? cursor : ms.getLast().seq();
                long readSeq = conversations.membership(c.id(), userId).map(Membership::readSeq).orElse(0L);
                out.add(Frames.of("CONVERSATION", "conversationId", c.id(), "kind", c.kind(), "messages", ms,
                        "hasMore", lastReturned < c.lastSeq(), "lastSeq", c.lastSeq(), "readSeq", readSeq));
            }
            return out;
        } finally {
            syncSlots.release();
        }
    }

    /**
     * Hafif kontrol: mesaj içeriği yok, sadece her sohbetin son seq'i ve kullanıcının okuma imleci. İstemci uygulama
     * ön plana gelince, RESYNC alınca ve açıkken dakikada bir çağırır; sadece kendisindeki seq'ten ileride olan
     * sohbetler için SYNC ister. Diğer bütün önlemler bir şekilde atlansa bile gecikmenin üst sınırı bu aralıktır.
     */
    public List<Map<String, Object>> heads(long userId) {
        return jdbc.sql("""
                        SELECT c.id, c.last_seq, m.read_seq FROM conversations c
                        JOIN conversation_members m ON m.conversation_id = c.id WHERE m.user_id = ?""")
                .param(userId)
                .query((rs, n) -> Frames.of("HEAD", "conversationId", rs.getLong(1), "lastSeq", rs.getLong(2),
                        "readSeq", rs.getLong(3)))
                .list();
    }

    public List<MessageView> history(long userId, long conversationId, long beforeSeq, int limit) {
        memberConversation(conversationId, userId);
        return messages.before(conversationId, beforeSeq, Math.min(Math.max(limit, 1), 100));
    }
}
