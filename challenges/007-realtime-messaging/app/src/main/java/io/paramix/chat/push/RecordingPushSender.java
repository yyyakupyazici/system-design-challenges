package io.paramix.chat.push;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

/** Lokal/test: bildirimleri kaydeder ve loglar. */
@Component
@ConditionalOnMissingBean(name = "pushSender")
public class RecordingPushSender implements PushSender {

    private static final Logger log = LoggerFactory.getLogger(RecordingPushSender.class);

    public record Push(long userId, long conversationId, String preview) {
    }

    private final List<Push> sent = new CopyOnWriteArrayList<>();

    @Override
    public void send(long userId, long conversationId, String preview) {
        sent.add(new Push(userId, conversationId, preview));
        log.info("PUSH → kullanıcı {} (sohbet {}): {}", userId, conversationId, preview);
    }

    public List<Push> sent() {
        return List.copyOf(sent);
    }
}
