package io.paramix.llmgw.pii;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bir isteğin yer tutucu ↔ gerçek değer eşlemesi. Sadece istek süresince bellekte yaşar;
 * hiçbir yere (cache, log, sağlayıcı) yazılmaz.
 */
public final class PiiVault {

    static final Pattern PLACEHOLDER = Pattern.compile("<(TCKN|IBAN|CARD|PHONE|EMAIL)_(\\d{1,3})>");

    private final Map<String, String> byPlaceholder = new LinkedHashMap<>();
    private final Map<String, String> byValue = new LinkedHashMap<>();
    private final Map<String, Integer> counters = new LinkedHashMap<>();

    /** Aynı değer aynı istekte hep aynı yer tutucuyu alır: model "<TCKN_1>" ile tutarlı akıl yürütür. */
    String placeholderFor(String type, String value) {
        String key = type + "|" + value;
        String existing = byValue.get(key);
        if (existing != null) {
            return existing;
        }
        int n = counters.merge(type, 1, Integer::sum);
        String placeholder = "<" + type + "_" + n + ">";
        byValue.put(key, placeholder);
        byPlaceholder.put(placeholder, value);
        return placeholder;
    }

    public String unmask(String text) {
        if (text == null || byPlaceholder.isEmpty()) {
            return text;
        }
        Matcher m = PLACEHOLDER.matcher(text);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String original = byPlaceholder.get(m.group());
            m.appendReplacement(out, Matcher.quoteReplacement(original != null ? original : m.group()));
        }
        m.appendTail(out);
        return out.toString();
    }

    public int size() {
        return byPlaceholder.size();
    }

    public boolean isEmpty() {
        return byPlaceholder.isEmpty();
    }
}
