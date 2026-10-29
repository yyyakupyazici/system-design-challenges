package io.paramix.notify.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Topic isimleri.
 * <ul>
 *   <li>Giriş (öncelik): {@code notif.critical}, {@code notif.transactional}, {@code notif.marketing}</li>
 *   <li>Teslimat (kanal × öncelik): {@code sms.critical}, {@code email.marketing}, ...</li>
 * </ul>
 */
public final class Topics {

    private Topics() {
    }

    public static String ingress(Priority priority) {
        return "notif." + priority.topicSuffix();
    }

    public static String delivery(Channel channel, Priority priority) {
        return channel.topicPrefix() + "." + priority.topicSuffix();
    }

    public static List<String> all() {
        List<String> topics = new ArrayList<>();
        for (Priority p : Priority.values()) {
            topics.add(ingress(p));
            for (Channel c : Channel.values()) {
                topics.add(delivery(c, p));
            }
        }
        return topics;
    }
}
