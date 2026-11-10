package io.paramix.chat.payment;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.message.MessageRepository;
import io.paramix.chat.message.MessageView;
import io.paramix.chat.outbox.ChatEvent;
import io.paramix.chat.outbox.OutboxWriter;
import io.paramix.chat.protocol.Frames;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Para isteğinin durum değişikliği sohbete bir SİSTEM mesajı olarak düşer ("✅ 250,00 TL ödendi"), çağıranın
 * transaction'ı içinde: durum değişikliği ile mesaj birlikte yazılır ya da hiçbiri. İstemci kartı bu mesajdaki
 * güncel durumla günceller; "ödendi ama sohbette bekliyor görünüyor" durumu oluşamaz.
 *
 * <p>clientMsgId deterministik (aynı olay için hep aynı UUID): reconciler aynı geçişi iki kez yapmaya çalışsa da
 * mesaj bir kez yazılır.
 */
@Component
public class SystemMessages {

    private final ConversationRepository conversations;
    private final MessageRepository messages;
    private final OutboxWriter outbox;

    public SystemMessages(ConversationRepository conversations, MessageRepository messages, OutboxWriter outbox) {
        this.conversations = conversations;
        this.messages = messages;
        this.outbox = outbox;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(long conversationId, long paymentRequestId, String event, String body, String pushPreview) {
        String kind = conversations.find(conversationId).orElseThrow().kind();
        UUID id = UUID.nameUUIDFromBytes(("payreq:" + paymentRequestId + ":" + event).getBytes(StandardCharsets.UTF_8));
        long seq = messages.nextSeq(conversationId);
        if (!messages.insert(conversationId, seq, 0, id, "SYSTEM", body, paymentRequestId)) {
            throw new IllegalStateException("sistem mesajı zaten yazılmış: " + id);
        }
        MessageView v = messages.bySeq(conversationId, seq).orElseThrow();
        outbox.write(new ChatEvent(conversationId, kind, 0, null, pushPreview, Frames.msg(v)));
    }
}
