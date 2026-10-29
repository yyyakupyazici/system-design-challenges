package io.paramix.notify.router;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@code {{degisken}}} yer tutucularını doldurur. Eksik parametre hatadır: yarım mesaj göndermektense hiç gönderme. */
public final class TemplateRenderer {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*([a-zA-Z0-9_]+)\\s*}}");

    private TemplateRenderer() {
    }

    public static String render(String template, Map<String, String> params) {
        if (template == null) {
            return null;
        }
        Matcher m = PLACEHOLDER.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            String value = params == null ? null : params.get(name);
            if (value == null) {
                throw new MissingParameterException(name);
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }

    public static class MissingParameterException extends RuntimeException {
        public MissingParameterException(String name) {
            super("Şablon parametresi eksik: " + name);
        }
    }
}
