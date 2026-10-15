package io.paylink.gateway.admin;

import io.paylink.gateway.config.PaylinkProperties;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * /admin/** için basit paylaşılan-token koruması (token K8s Secret'tan gelir).
 * Prod'da: ayrı bir iç Ingress/NetworkPolicy + SSO/OIDC tercih edilmeli.
 */
@Component
public class AdminTokenWebFilter implements WebFilter {

    static final String ADMIN_TOKEN_HEADER = "X-Admin-Token";

    private final byte[] expected;

    public AdminTokenWebFilter(PaylinkProperties props) {
        this.expected = props.adminToken().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!exchange.getRequest().getPath().pathWithinApplication().value().startsWith("/admin")) {
            return chain.filter(exchange);
        }
        String token = exchange.getRequest().getHeaders().getFirst(ADMIN_TOKEN_HEADER);
        // Sabit zamanlı karşılaştırma (timing attack'a karşı)
        if (token == null || !MessageDigest.isEqual(expected, token.getBytes(StandardCharsets.UTF_8))) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange);
    }
}
