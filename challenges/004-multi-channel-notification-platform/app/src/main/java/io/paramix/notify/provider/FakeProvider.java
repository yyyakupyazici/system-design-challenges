package io.paramix.notify.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import io.paramix.notify.domain.Channel;

/**
 * Bellek içi sağlayıcı. Testler sonraki cevapları senaryolayabilir (timeout, 429, kesinti...).
 * {@link #handsetMessages()} kullanıcının telefonuna GERÇEKTEN düşen mesajları tutar: mükerrer gönderim
 * testleri bunu sayar.
 */
public class FakeProvider implements ChannelProvider {

    public enum Outcome { ACCEPT, RATE_LIMIT, PERMANENT_FAIL, UNAVAILABLE, TIMEOUT_AFTER_SEND, TIMEOUT_BEFORE_SEND }

    private final String name;
    private final Channel channel;
    private final boolean dedupByClientRef;
    private final Map<String, String> msgIdByClientRef = new ConcurrentHashMap<>();
    private final List<OutboundMessage> handset = new CopyOnWriteArrayList<>();
    private final Queue<Outcome> script = new ConcurrentLinkedQueue<>();
    private volatile boolean lookupUnavailable;

    public FakeProvider(String name, Channel channel, boolean dedupByClientRef) {
        this.name = name;
        this.channel = channel;
        this.dedupByClientRef = dedupByClientRef;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public SendResult send(OutboundMessage m) {
        Outcome outcome = script.poll();
        if (outcome == null) {
            outcome = Outcome.ACCEPT;
        }
        return switch (outcome) {
            case ACCEPT -> new SendResult.Accepted(deliver(m));
            case TIMEOUT_AFTER_SEND -> {
                deliver(m);   // mesaj telefona düştü, ama cevap bize ulaşmadı
                yield new SendResult.Timeout("read timeout after send");
            }
            case TIMEOUT_BEFORE_SEND -> new SendResult.Timeout("read timeout (not sent)");
            case RATE_LIMIT -> new SendResult.RateLimited("429 Too Many Requests");
            case PERMANENT_FAIL -> new SendResult.PermanentFailure("invalid_recipient");
            case UNAVAILABLE -> new SendResult.Unavailable("503 Service Unavailable");
        };
    }

    /** Sağlayıcı tarafı idempotency: aynı clientRef ikinci kez gelirse yeni mesaj gönderilmez. */
    private String deliver(OutboundMessage m) {
        if (dedupByClientRef) {
            String existing = msgIdByClientRef.get(m.clientRef());
            if (existing != null) {
                return existing;
            }
        }
        String msgId = name + "-" + UUID.randomUUID();
        msgIdByClientRef.put(m.clientRef(), msgId);
        handset.add(m);
        return msgId;
    }

    @Override
    public LookupResult lookup(String clientRef) {
        if (lookupUnavailable) {
            return new LookupResult.Unavailable("status API down");
        }
        String msgId = msgIdByClientRef.get(clientRef);
        return msgId == null ? new LookupResult.NotFound() : new LookupResult.Found(msgId);
    }

    // --- test kancaları ---

    public void script(Outcome... outcomes) {
        script.addAll(List.of(outcomes));
    }

    public void setLookupUnavailable(boolean unavailable) {
        this.lookupUnavailable = unavailable;
    }

    public List<OutboundMessage> handsetMessages() {
        return new ArrayList<>(handset);
    }

    public long handsetCount(String clientRef) {
        return handset.stream().filter(m -> m.clientRef().equals(clientRef)).count();
    }

    public String providerMsgId(String clientRef) {
        return msgIdByClientRef.get(clientRef);
    }

    public void reset() {
        script.clear();
        lookupUnavailable = false;
    }
}
