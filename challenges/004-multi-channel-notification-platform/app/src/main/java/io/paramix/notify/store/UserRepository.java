package io.paramix.notify.store;

import java.sql.Time;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class UserRepository {

    public record PreferenceUpdate(Boolean marketingConsent, Boolean smsEnabled, Boolean emailEnabled,
                                   Boolean pushEnabled, LocalTime quietStart, LocalTime quietEnd) {
    }

    private final JdbcClient jdbc;

    public UserRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<UserProfile> load(String userId) {
        List<String> tokens = jdbc.sql("SELECT token FROM device_token WHERE user_id = :uid ORDER BY installation_id")
                .param("uid", userId).query(String.class).list();
        return jdbc.sql("""
                        SELECT u.user_id, u.phone, u.email, u.locale, u.time_zone,
                               COALESCE(p.marketing_consent, FALSE) AS marketing_consent,
                               COALESCE(p.sms_enabled, TRUE) AS sms_enabled,
                               COALESCE(p.email_enabled, TRUE) AS email_enabled,
                               COALESCE(p.push_enabled, TRUE) AS push_enabled,
                               COALESCE(p.quiet_start, TIME '22:00') AS quiet_start,
                               COALESCE(p.quiet_end, TIME '08:00') AS quiet_end
                        FROM app_user u LEFT JOIN user_preference p ON p.user_id = u.user_id
                        WHERE u.user_id = :uid""")
                .param("uid", userId)
                .query((rs, i) -> new UserProfile(
                        rs.getString("user_id"), rs.getString("phone"), rs.getString("email"),
                        rs.getString("locale"), rs.getString("time_zone"),
                        rs.getBoolean("marketing_consent"), rs.getBoolean("sms_enabled"),
                        rs.getBoolean("email_enabled"), rs.getBoolean("push_enabled"),
                        toLocal(rs.getTime("quiet_start")), toLocal(rs.getTime("quiet_end")),
                        tokens))
                .optional();
    }

    /** Göndermeden hemen önce, cache'siz: kuyrukta beklerken izin geri çekildi mi? */
    public boolean marketingAllowedNow(String userId, String channel) {
        String column = switch (channel) {
            case "SMS" -> "sms_enabled";
            case "EMAIL" -> "email_enabled";
            case "PUSH" -> "push_enabled";
            default -> throw new IllegalArgumentException(channel);
        };
        return jdbc.sql("SELECT marketing_consent AND " + column + " FROM user_preference WHERE user_id = :uid")
                .param("uid", userId).query(Boolean.class).optional().orElse(false);
    }

    @Transactional
    public void updatePreferences(String userId, PreferenceUpdate u) {
        jdbc.sql("INSERT INTO user_preference (user_id) VALUES (:uid) ON CONFLICT (user_id) DO NOTHING")
                .param("uid", userId).update();
        jdbc.sql("""
                        UPDATE user_preference SET
                            marketing_consent = COALESCE(:mc, marketing_consent),
                            sms_enabled       = COALESCE(:sms, sms_enabled),
                            email_enabled     = COALESCE(:email, email_enabled),
                            push_enabled      = COALESCE(:push, push_enabled),
                            quiet_start       = COALESCE(:qs, quiet_start),
                            quiet_end         = COALESCE(:qe, quiet_end),
                            updated_at        = now()
                        WHERE user_id = :uid""")
                .param("mc", u.marketingConsent())
                .param("sms", u.smsEnabled())
                .param("email", u.emailEnabled())
                .param("push", u.pushEnabled())
                .param("qs", u.quietStart() == null ? null : Time.valueOf(u.quietStart()))
                .param("qe", u.quietEnd() == null ? null : Time.valueOf(u.quietEnd()))
                .param("uid", userId)
                .update();
    }

    /** Kurulum bazında upsert. Dönüş: kurulumun önceki sahibi (cache'i o kullanıcı için de silmek gerekir). */
    @Transactional
    public Optional<String> upsertDevice(String installationId, String userId, String platform, String token) {
        Optional<String> previousOwner = jdbc.sql("SELECT user_id FROM device_token WHERE installation_id = :iid FOR UPDATE")
                .param("iid", installationId).query(String.class).optional();
        jdbc.sql("""
                        INSERT INTO device_token (installation_id, user_id, platform, token)
                        VALUES (:iid, :uid, :platform, :token)
                        ON CONFLICT (installation_id) DO UPDATE
                           SET user_id = EXCLUDED.user_id, platform = EXCLUDED.platform, token = EXCLUDED.token,
                               updated_at = now(), last_seen_at = now()""")
                .param("iid", installationId).param("uid", userId).param("platform", platform).param("token", token)
                .update();
        return previousOwner;
    }

    private static LocalTime toLocal(Time t) {
        return t == null ? null : t.toLocalTime();
    }
}
