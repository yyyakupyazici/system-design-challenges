package io.paramix.llmgw.cache;

import java.util.List;
import java.util.Locale;

import io.paramix.llmgw.api.ChatCompletionRequest;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.config.GatewayProperties.CacheProperties;

/**
 * Kural: cevap SADECE soruya bağlıysa cache'lenir; soran kişiye, onun verisine ya da bir eyleme bağlıysa asla.
 *
 * <pre>
 *  (a) "Kartımı nasıl iptal ederim?"            → genel bilgi        → cache ✓ (exact + semantic)
 *  (b) "Hesabımda ne kadar para var?"            → kişiye özel veri   → cache ✗
 *  (c) "TR12... IBAN'a 500 TL gönder"            → eylem + PII        → cache ✗
 *  (d) "EFT ile havale farkı nedir?"             → genel bilgi        → cache ✓
 *  (e) "Bu ayki harcamalarımı özetle"            → kişiye özel veri   → cache ✗
 *  (f) gizli etiketli istek                       → veri sınıfı        → cache ✗
 * </pre>
 *
 * Belirsizlikte kapalı tarafta kalır: yanlış cevap dönmek (başkasının bakiyesi!) hiç dönmemekten kötü.
 */
public final class CachePolicy {

    private CachePolicy() {
    }

    public static CacheDecision decide(ClientContext ctx, ChatCompletionRequest req, String maskedLastUser,
                                       int piiCount, CacheProperties props) {
        if (ctx.confidential()) {
            return CacheDecision.none("confidential");
        }
        if (!ctx.appCacheable()) {
            return CacheDecision.none("app_opt_out");
        }
        if (req.temperature() != null && req.temperature() > 0.3) {
            return CacheDecision.none("high_temperature");   // çağıran her seferinde farklı cevap istiyor
        }
        if (piiCount > 0) {
            return CacheDecision.none("contains_pii");
        }
        String text = maskedLastUser == null ? "" : maskedLastUser.toLowerCase(new Locale("tr"));
        if (containsAny(text, props.personalMarkers())) {
            return CacheDecision.none("personal");
        }
        if (containsAny(text, props.actionMarkers())) {
            return CacheDecision.none("action");
        }
        // Semantic cache sadece tek soruluk isteklerde: sohbetin ortasındaki "peki ya ikincisi?" bağlamsız anlamsız.
        long userTurns = req.messages().stream().filter(m -> "user".equals(m.role())).count();
        long assistantTurns = req.messages().stream().filter(m -> "assistant".equals(m.role())).count();
        boolean singleTurn = userTurns == 1 && assistantTurns == 0;
        return new CacheDecision(true, props.semanticEnabled() && singleTurn, null);
    }

    private static boolean containsAny(String text, List<String> markers) {
        for (String m : markers) {
            if (text.contains(m.toLowerCase(new Locale("tr")))) {
                return true;
            }
        }
        return false;
    }
}
