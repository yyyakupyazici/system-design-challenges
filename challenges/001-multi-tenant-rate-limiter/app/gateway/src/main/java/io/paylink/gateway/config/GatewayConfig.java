package io.paylink.gateway.config;

import java.time.Clock;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayConfig {

    /** Public API rotaları. Rate limit GlobalFilter ile tüm rotalara uygulanır. */
    @Bean
    RouteLocator routes(RouteLocatorBuilder builder, PaylinkProperties props) {
        return builder.routes()
                .route("payments", r -> r.path("/payments", "/payments/**")
                        .uri(props.paymentServiceUrl()))
                .build();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
