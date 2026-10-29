package io.paramix.notify.router;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.NotificationEvent;
import io.paramix.notify.domain.Priority;
import io.paramix.notify.store.UserProfile;

/**
 * Router'ın karar mantığı. Saf fonksiyon (I/O yok): birim testlerle tüm kurallar doğrulanır.
 *
 * <ul>
 *   <li>CRITICAL (OTP): tercih ve sessiz saat dinlemez, iletişim bilgisi varsa gider.</li>
 *   <li>TRANSACTIONAL: kanal kapalıysa o kanaldan gitmez.</li>
 *   <li>MARKETING: pazarlama izni şart; sessiz saatte {@code SCHEDULED} olur.</li>
 *   <li>fallback=true: ilk kullanılabilir kanal hemen, diğerleri {@code FALLBACK_WAIT}
 *       (ör. önce push, ack gelmezse SMS).</li>
 * </ul>
 */
public final class RoutingPolicy {

    private RoutingPolicy() {
    }

    public static List<Channel> defaultChannels(Priority priority) {
        return priority == Priority.CRITICAL ? List.of(Channel.SMS) : List.of(Channel.PUSH, Channel.EMAIL);
    }

    public static List<ChannelPlan> decide(NotificationEvent e, UserProfile user, Instant now, Duration fallbackWait) {
        List<Channel> requested = e.channels() == null || e.channels().isEmpty()
                ? defaultChannels(e.priority()) : e.channels();
        boolean fallbackMode = e.fallback() && e.priority() != Priority.MARKETING;

        List<ChannelPlan> plans = new ArrayList<>();
        boolean primaryChosen = false;
        for (Channel channel : requested) {
            String recipient = user.recipient(channel);
            if (recipient == null) {
                plans.add(ChannelPlan.skipped(channel, "no_contact"));
                continue;
            }
            if (e.priority() != Priority.CRITICAL && !user.channelEnabled(channel)) {
                plans.add(ChannelPlan.skipped(channel, "channel_opt_out"));
                continue;
            }
            if (e.priority() == Priority.MARKETING) {
                if (!user.marketingConsent()) {
                    plans.add(ChannelPlan.skipped(channel, "no_marketing_consent"));
                    continue;
                }
                Instant quietEnd = quietHoursEnd(user, now);
                if (quietEnd != null) {
                    plans.add(new ChannelPlan(channel, recipient, DeliveryStatus.SCHEDULED, quietEnd, "quiet_hours"));
                    continue;
                }
            }
            if (fallbackMode && primaryChosen) {
                plans.add(new ChannelPlan(channel, recipient, DeliveryStatus.FALLBACK_WAIT,
                        now.plus(fallbackWait), "fallback"));
                continue;
            }
            plans.add(ChannelPlan.pending(channel, recipient));
            primaryChosen = true;
        }
        return plans;
    }

    /**
     * Kullanıcının saat diliminde sessiz saatteyse sessiz saatin bittiği an, değilse null.
     * start == end ise sessiz saat yok demektir. 22:00–08:00 gibi gece yarısını aşan aralıkları destekler.
     */
    public static Instant quietHoursEnd(UserProfile user, Instant now) {
        LocalTime start = user.quietStart();
        LocalTime end = user.quietEnd();
        if (start == null || end == null || start.equals(end)) {
            return null;
        }
        ZoneId zone = ZoneId.of(user.timeZone() == null ? "Europe/Istanbul" : user.timeZone());
        ZonedDateTime local = now.atZone(zone);
        LocalTime t = local.toLocalTime();

        boolean crossesMidnight = start.isAfter(end);
        boolean quiet = crossesMidnight
                ? !t.isBefore(start) || t.isBefore(end)
                : !t.isBefore(start) && t.isBefore(end);
        if (!quiet) {
            return null;
        }
        ZonedDateTime endToday = local.with(end).withSecond(0).withNano(0);
        ZonedDateTime endAt = endToday.isAfter(local) ? endToday : endToday.plusDays(1);
        return endAt.toInstant();
    }
}
