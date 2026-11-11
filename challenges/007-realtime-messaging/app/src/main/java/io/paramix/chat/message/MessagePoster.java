package io.paramix.chat.message;

import java.util.UUID;

import io.paramix.chat.outbox.ChatEvent;
import io.paramix.chat.outbox.OutboxWriter;
import io.paramix.chat.protocol.Frames;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Sunucunun kendi adına (istemci soketi olmadan) sohbete mesaj yazması: para isteği kartı ve durum mesajları.
 * Kullanıcı mesajıyla AYNI yol: sıra numarası, messages, outbox → Kafka → fan-out. Çağıranın transaction'ında çalışır.
 *
 * <p>clientMsgId çağıran tarafından deterministik verilir (aynı olay için hep aynı UUID): olay iki kez işlense de
 * mesaj bir kez yazılır.
 */
@Component
public class MessagePoster {

    private final ConversationRepository conversations;
    private final MessageRepository messages;
    private final OutboxWriter outbox;

    public MessagePoster(ConversationRepository conversations, MessageRepository messages, OutboxWriter outbox) {
        this.conversations = conversations;
        this.messages = messages;
        this.outbox = outbox;
    }

    /** Dönüş: yazıldıysa true, bu clientMsgId zaten varsa false (o durumda sıra numarası tüketilmez). */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean post(long conversationId, long senderId, UUID clientMsgId, String kind, String body,
                        Long paymentRequestId, String pushPreview) {
        if (messages.byClientId(conversationId, clientMsgId).isPresent()) {
            return false;
        }
        String convKind = conversations.find(conversationId).orElseThrow().kind();
        long seq = messages.nextSeq(conversationId);           // satır kilidi: aynı sohbete yazanlar burada sıraya girer
        if (!messages.insert(conversationId, seq, senderId, clientMsgId, kind, body, paymentRequestId)) {
            throw new IllegalStateException("mesaj aynı anda iki kez yazıldı: " + clientMsgId);   // tx geri alınır, olay tekrar denenir
        }
        MessageView v = messages.bySeq(conversationId, seq).orElseThrow();
        outbox.write(new ChatEvent(conversationId, convKind, senderId, null, pushPreview, Frames.msg(v)));
        return true;
    }
}
