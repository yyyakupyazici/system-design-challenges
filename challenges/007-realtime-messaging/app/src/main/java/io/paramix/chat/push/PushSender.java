package io.paramix.chat.push;

/** Çevrimdışı alıcıya bildirim. Üretimde 004'teki bildirim platformuna (POST /notifications, PUSH kanalı) gider. */
public interface PushSender {

    void send(long userId, long conversationId, String preview);
}
