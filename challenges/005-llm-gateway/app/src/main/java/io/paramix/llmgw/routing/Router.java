package io.paramix.llmgw.routing;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.ModelProperties;
import io.paramix.llmgw.config.GatewayProperties.Tier;
import io.paramix.llmgw.core.GatewayException;

import org.springframework.http.HttpStatus;

/**
 * Hangi model, hangi sırayla? Saf mantık (I/O yok), birim testli.
 *
 * <ul>
 *   <li>{@code auto}: uzun girdi ya da "analiz, kod, karşılaştır..." gibi zor iş işaretleri → büyük model;
 *       gerisi → küçük model. Tasarrufun en büyük kaldıracı bu karar.</li>
 *   <li>Gizli veri: sadece internal (kendi GPU kümemiz) sağlayıcılar. Açıkça dış model istenirse reddedilir;
 *       gizli veri hiçbir koşulda (fallback dahil) dışarı çıkmaz.</li>
 *   <li>Fallback zinciri: modelin config'teki yedekleri, aynı veri sınıfı kuralıyla süzülmüş.</li>
 * </ul>
 */
public final class Router {

    public record Plan(String primary, List<String> candidates) {
    }

    private Router() {
    }

    public static Plan route(GatewayProperties props, String requestedModel, boolean confidential,
                             int inputTokens, String lastUserMessage) {
        String primary;
        if ("auto".equalsIgnoreCase(requestedModel)) {
            primary = confidential ? props.routing().confidentialModel() : autoPick(props, inputTokens, lastUserMessage);
        } else {
            primary = requestedModel;
            if (!props.models().containsKey(primary)) {
                throw new GatewayException(HttpStatus.NOT_FOUND, "model_not_found", "Bilinmeyen model: " + primary);
            }
            if (confidential && !internal(props, primary)) {
                throw new GatewayException(HttpStatus.FORBIDDEN, "data_class_violation",
                        "Gizli veri sadece kendi altyapımızdaki modellere gidebilir; 'auto' ya da internal bir model kullanın");
            }
        }
        if (!props.models().containsKey(primary)) {
            throw new GatewayException(HttpStatus.INTERNAL_SERVER_ERROR, "routing_misconfigured", "Yönlendirme modeli tanımsız: " + primary);
        }
        Set<String> ordered = new LinkedHashSet<>();
        ordered.add(primary);
        ordered.addAll(props.models().get(primary).fallbacks());
        List<String> candidates = new ArrayList<>();
        for (String alias : ordered) {
            if (!confidential || internal(props, alias)) {
                candidates.add(alias);
            }
        }
        return new Plan(primary, List.copyOf(candidates));
    }

    static String autoPick(GatewayProperties props, int inputTokens, String lastUserMessage) {
        if (inputTokens >= props.routing().largeInputTokens()) {
            return props.routing().largeModel();
        }
        String text = lastUserMessage == null ? "" : lastUserMessage.toLowerCase(new Locale("tr"));
        for (String k : props.routing().largeKeywords()) {
            if (text.contains(k.toLowerCase(new Locale("tr")))) {
                return props.routing().largeModel();
            }
        }
        return props.routing().smallModel();
    }

    /** Bütçe biterken "ucuz modele düşür" seçeneği: veri sınıfına uygun en ucuz SMALL/INTERNAL model. */
    public static Optional<String> cheapest(GatewayProperties props, boolean confidential) {
        return props.models().entrySet().stream()
                .filter(e -> e.getValue().tier() != Tier.LARGE)
                .filter(e -> !confidential || internal(props, e.getKey()))
                .min(Comparator.comparingLong(e -> e.getValue().inputPrice() + e.getValue().outputPrice()))
                .map(java.util.Map.Entry::getKey);
    }

    public static boolean internal(GatewayProperties props, String alias) {
        ModelProperties m = props.models().get(alias);
        return m != null && props.providers().get(m.provider()).internal();
    }
}
