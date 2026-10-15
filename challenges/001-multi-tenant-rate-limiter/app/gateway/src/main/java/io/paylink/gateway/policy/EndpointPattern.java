package io.paylink.gateway.policy;

import java.util.Comparator;
import java.util.Locale;
import org.springframework.http.server.PathContainer;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

/**
 * Kuraldaki endpoint ifadesi.
 * <ul>
 *   <li>{@code *} : genel kova (tüm endpoint'ler)</li>
 *   <li>{@code POST /payments} : metod + path</li>
 *   <li>{@code * /payments/**} : her metod + path pattern</li>
 * </ul>
 */
public final class EndpointPattern {

    public static final String ANY = "*";

    /** Daha spesifik olan önce: önce path pattern'i, sonra somut metod ('*' yerine). */
    public static final Comparator<EndpointPattern> MOST_SPECIFIC_FIRST =
            Comparator.<EndpointPattern, PathPattern>comparing(p -> p.pathPattern, PathPattern.SPECIFICITY_COMPARATOR)
                    .thenComparing(p -> ANY.equals(p.method));

    private static final PathPatternParser PARSER = PathPatternParser.defaultInstance;

    private final String raw;
    private final String method;
    private final PathPattern pathPattern;

    private EndpointPattern(String raw, String method, PathPattern pathPattern) {
        this.raw = raw;
        this.method = method;
        this.pathPattern = pathPattern;
    }

    public static EndpointPattern parse(String raw) {
        String trimmed = raw == null ? "" : raw.trim();
        if (trimmed.isEmpty() || ANY.equals(trimmed)) {
            return new EndpointPattern(ANY, ANY, null);
        }
        String[] parts = trimmed.split("\\s+", 2);
        if (parts.length != 2 || !parts[1].startsWith("/")) {
            throw new IllegalArgumentException(
                    "Endpoint '*' ya da '<METHOD> /path' biçiminde olmalı: " + raw);
        }
        String method = parts[0].toUpperCase(Locale.ROOT);
        return new EndpointPattern(method + " " + parts[1], method, PARSER.parse(parts[1]));
    }

    public boolean isAny() {
        return pathPattern == null;
    }

    public boolean matches(String httpMethod, String path) {
        if (isAny()) {
            return true;
        }
        boolean methodOk = ANY.equals(method) || method.equalsIgnoreCase(httpMethod);
        return methodOk && pathPattern.matches(PathContainer.parsePath(path));
    }

    /** Normalize edilmiş ifade; Redis key'inde de kullanılır. */
    public String raw() {
        return raw;
    }

    @Override
    public String toString() {
        return raw;
    }
}
