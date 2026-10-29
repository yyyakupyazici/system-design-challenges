package io.paramix.notify.router;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import io.paramix.notify.domain.Channel;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.NotificationEvent;
import io.paramix.notify.domain.Priority;
import io.paramix.notify.store.UserProfile;

import org.junit.jupiter.api.Test;

class RoutingPolicyTest {

    // 2026-10-29 11:00 UTC = 14:00 İstanbul (sessiz saat dışında)
    private static final Instant NOON = Instant.parse("2026-10-29T11:00:00Z");
    // 2026-10-29 20:30 UTC = 23:30 İstanbul (sessiz saat içinde)
    private static final Instant NIGHT = Instant.parse("2026-10-29T20:30:00Z");
    private static final Duration FALLBACK = Duration.ofSeconds(10);

    @Test
    void critical_ignoresChannelOptOutAndQuietHours() {
        UserProfile user = user(false, false, true, true);
        List<ChannelPlan> plans = RoutingPolicy.decide(event(Priority.CRITICAL, List.of(Channel.SMS), false), user, NIGHT, FALLBACK);

        assertThat(plans).singleElement().satisfies(p -> {
            assertThat(p.status()).isEqualTo(DeliveryStatus.PENDING);
            assertThat(p.recipient()).isEqualTo("+905550000001");
        });
    }

    @Test
    void transactional_respectsChannelOptOut() {
        UserProfile user = user(true, false, true, true);   // SMS kapalı
        List<ChannelPlan> plans = RoutingPolicy.decide(
                event(Priority.TRANSACTIONAL, List.of(Channel.SMS, Channel.PUSH), false), user, NOON, FALLBACK);

        assertThat(plans).extracting(ChannelPlan::channel, ChannelPlan::status, ChannelPlan::reason).containsExactly(
                org.assertj.core.groups.Tuple.tuple(Channel.SMS, DeliveryStatus.SKIPPED, "channel_opt_out"),
                org.assertj.core.groups.Tuple.tuple(Channel.PUSH, DeliveryStatus.PENDING, null));
    }

    @Test
    void marketing_withoutConsent_isSkipped() {
        UserProfile user = user(false, true, true, true);
        List<ChannelPlan> plans = RoutingPolicy.decide(event(Priority.MARKETING, List.of(Channel.SMS), false), user, NOON, FALLBACK);

        assertThat(plans).singleElement().satisfies(p -> {
            assertThat(p.status()).isEqualTo(DeliveryStatus.SKIPPED);
            assertThat(p.reason()).isEqualTo("no_marketing_consent");
        });
    }

    @Test
    void marketing_inQuietHours_isScheduledUntilQuietEndInUserTimeZone() {
        UserProfile user = user(true, true, true, true);
        List<ChannelPlan> plans = RoutingPolicy.decide(event(Priority.MARKETING, List.of(Channel.PUSH), false), user, NIGHT, FALLBACK);

        assertThat(plans).singleElement().satisfies(p -> {
            assertThat(p.status()).isEqualTo(DeliveryStatus.SCHEDULED);
            // Ertesi gün 08:00 İstanbul = 05:00 UTC
            assertThat(p.notBefore()).isEqualTo(Instant.parse("2026-10-30T05:00:00Z"));
        });
    }

    @Test
    void quietHours_afterMidnightEndsSameDay() {
        UserProfile user = user(true, true, true, true);
        Instant at0230Istanbul = Instant.parse("2026-10-29T23:30:00Z"); // 30 Ekim 02:30 İstanbul
        assertThat(RoutingPolicy.quietHoursEnd(user, at0230Istanbul)).isEqualTo(Instant.parse("2026-10-30T05:00:00Z"));
        assertThat(RoutingPolicy.quietHoursEnd(user, NOON)).isNull();
    }

    @Test
    void quietHours_equalStartAndEnd_meansNoQuietHours() {
        UserProfile u = new UserProfile("u", "+90", "a@b", "tr", "Europe/Istanbul", true, true, true, true,
                LocalTime.MIDNIGHT, LocalTime.MIDNIGHT, List.of("tok"));
        assertThat(RoutingPolicy.quietHoursEnd(u, NIGHT)).isNull();
    }

    @Test
    void fallbackMode_firstChannelNow_restWaitForAck() {
        UserProfile user = user(true, true, true, true);
        List<ChannelPlan> plans = RoutingPolicy.decide(
                event(Priority.CRITICAL, List.of(Channel.PUSH, Channel.SMS), true), user, NOON, FALLBACK);

        assertThat(plans).extracting(ChannelPlan::channel, ChannelPlan::status).containsExactly(
                org.assertj.core.groups.Tuple.tuple(Channel.PUSH, DeliveryStatus.PENDING),
                org.assertj.core.groups.Tuple.tuple(Channel.SMS, DeliveryStatus.FALLBACK_WAIT));
        assertThat(plans.get(1).notBefore()).isEqualTo(NOON.plus(FALLBACK));
    }

    @Test
    void fallbackMode_withoutPushToken_smsBecomesPrimary() {
        UserProfile noTokens = new UserProfile("u_1", "+905550000001", "a@b", "tr", "Europe/Istanbul",
                true, true, true, true, LocalTime.of(22, 0), LocalTime.of(8, 0), List.of());
        List<ChannelPlan> plans = RoutingPolicy.decide(
                event(Priority.CRITICAL, List.of(Channel.PUSH, Channel.SMS), true), noTokens, NOON, FALLBACK);

        assertThat(plans).extracting(ChannelPlan::status)
                .containsExactly(DeliveryStatus.SKIPPED, DeliveryStatus.PENDING);
    }

    @Test
    void noChannels_usesPriorityDefaults() {
        assertThat(RoutingPolicy.defaultChannels(Priority.CRITICAL)).containsExactly(Channel.SMS);
        assertThat(RoutingPolicy.defaultChannels(Priority.MARKETING)).containsExactly(Channel.PUSH, Channel.EMAIL);
    }

    private static UserProfile user(boolean consent, boolean sms, boolean email, boolean push) {
        return new UserProfile("u_1", "+905550000001", "ayse@example.com", "tr", "Europe/Istanbul",
                consent, sms, email, push, LocalTime.of(22, 0), LocalTime.of(8, 0), List.of("apns-token-u1"));
    }

    private static NotificationEvent event(Priority p, List<Channel> channels, boolean fallback) {
        return new NotificationEvent("ntf_1", "key-1", "T", p, "u_1", "tpl", Map.of(), channels, fallback, null, NOON);
    }
}
