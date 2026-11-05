package io.paramix.llmgw.api;

import io.paramix.llmgw.auth.ApiKeyAuthenticator;
import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.core.GatewayService;

import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * OpenAI uyumlu uç: ekipler mevcut OpenAI SDK'larını sadece {@code base_url} değiştirerek kullanır.
 *
 * <ul>
 *   <li>{@code X-User-Id} — bütçede kişi başı garanti/tavan için.</li>
 *   <li>{@code X-Data-Class: confidential} — veri sadece kendi GPU kümemizdeki modele gider.</li>
 *   <li>{@code X-Session-Id} — sohbet oturumu; prompt caching kararında sinyal.</li>
 * </ul>
 */
@RestController
public class ChatCompletionController {

    private final ApiKeyAuthenticator auth;
    private final GatewayService gateway;

    public ChatCompletionController(ApiKeyAuthenticator auth, GatewayService gateway) {
        this.auth = auth;
        this.gateway = gateway;
    }

    @PostMapping(path = "/v1/chat/completions", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public Object complete(@RequestHeader(value = "Authorization", required = false) String authorization,
                           @RequestHeader(value = "X-User-Id", required = false) String userId,
                           @RequestHeader(value = "X-Data-Class", required = false) String dataClass,
                           @RequestHeader(value = "X-Session-Id", required = false) String sessionId,
                           @Valid @RequestBody ChatCompletionRequest req) {
        ClientContext ctx = auth.authenticate(authorization, userId, dataClass, sessionId);
        if (!req.stream()) {
            return gateway.complete(ctx, req);
        }
        // Reddedilecekse (bütçe, limit, yetki) bu çağrı stream açılmadan exception fırlatır → düzgün HTTP hatası.
        Flux<String> chunks = gateway.stream(ctx, req);
        return chunks.map(d -> ServerSentEvent.builder(d).build());
    }
}
