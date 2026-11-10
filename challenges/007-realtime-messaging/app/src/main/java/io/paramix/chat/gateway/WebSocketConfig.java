package io.paramix.chat.gateway;

import java.util.Map;

import io.paramix.chat.config.ChatProperties;
import io.paramix.chat.fanout.EphemeralRelay;
import io.paramix.chat.message.ConversationRepository;
import io.paramix.chat.message.MessageService;
import io.paramix.chat.payment.PaymentRequestService;
import io.paramix.chat.presence.PresenceService;
import io.paramix.chat.protocol.Json;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * {@code ws://host/ws?userId=42&deviceId=phone}
 *
 * <p>Kimlik doğrulama burada demo amaçlı sorgu parametresiyle. Üretimde el sıkışmada JWT doğrulanır
 * (Authorization header ya da ilk çerçeve) ve userId token'dan alınır; istemcinin söylediğine güvenilmez.
 *
 * <p>El sıkışmada kabul kontrolü: boşaltılıyorsa, bağlantı tavanı dolduysa ya da saniyelik kabul hızı aşıldıysa
 * 503 + rastgele Retry-After (geri bağlanma fırtınasını zamana yayar).
 */
@Configuration
@EnableWebSocket
@ConditionalOnProperty(name = "chat.roles.gateway", havingValue = "true", matchIfMissing = true)
public class WebSocketConfig implements WebSocketConfigurer {

    private final ChatWebSocketHandler handler;
    private final LocalSessions sessions;
    private final GatewayDrainer drainer;
    private final AdmissionLimiter limiter;
    private final ChatProperties props;
    private final MeterRegistry meters;

    public WebSocketConfig(LocalSessions sessions, RoutingTable routing, GatewayIdentity identity,
                           ConversationRepository conversations, MessageService messages, PaymentRequestService payments,
                           PresenceService presence, EphemeralRelay ephemeral, ChatProperties props, Json json,
                           MeterRegistry meters, GatewayDrainer drainer) {
        this.handler = new ChatWebSocketHandler(sessions, routing, identity, conversations, messages, payments, presence,
                ephemeral, props, json, meters);
        this.sessions = sessions;
        this.drainer = drainer;
        this.limiter = new AdmissionLimiter(props.gateway().handshakesPerSecond());
        this.props = props;
        this.meters = meters;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws").addInterceptors(new Admission()).setAllowedOriginPatterns("*");
    }

    private final class Admission implements HandshakeInterceptor {

        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler h,
                                       Map<String, Object> attrs) {
            var q = UriComponentsBuilder.fromUri(request.getURI()).build().getQueryParams();
            String user = q.getFirst("userId");
            String device = q.getFirst("deviceId");
            if (user == null || device == null || !user.matches("\\d+") || !device.matches("[A-Za-z0-9_-]{1,40}")) {
                response.setStatusCode(HttpStatus.UNAUTHORIZED);
                return false;
            }
            String reject = drainer.isDraining() ? "draining"
                    : sessions.size() >= props.gateway().maxConnections() ? "full"
                    : !limiter.tryAcquire() ? "rate" : null;
            if (reject != null) {
                meters.counter("chat.gateway.handshake_rejected", "reason", reject).increment();
                response.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                response.getHeaders().add("Retry-After", String.valueOf(AdmissionLimiter.retryAfterSeconds()));
                return false;
            }
            attrs.put(ChatWebSocketHandler.ATTR_USER, Long.parseLong(user));
            attrs.put(ChatWebSocketHandler.ATTR_DEVICE, device);
            return true;
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler h,
                                   Exception exception) {
        }
    }
}
