package io.paramix.chat.api;

import java.util.List;
import java.util.Map;

import io.paramix.chat.message.ChatException;
import io.paramix.chat.message.MessageService;
import io.paramix.chat.message.MessageView;
import io.paramix.chat.presence.PresenceService;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST: geçmiş (sayfalı kaydırma), presence ve demo amaçlı kullanıcı/sohbet oluşturma.
 * Gerçek zamanlı her şey WebSocket'te. Kimlik demo amaçlı X-User-Id header'ı (üretimde JWT).
 */
@RestController
public class ChatApiController {

    public record NewUser(long id, String name, boolean presenceHidden) {
    }

    public record Member(long userId, String role) {
    }

    public record NewConversation(long id, String kind, String title, List<Member> members) {
    }

    private final JdbcClient jdbc;
    private final MessageService messages;
    private final PresenceService presence;

    public ChatApiController(JdbcClient jdbc, MessageService messages, PresenceService presence) {
        this.jdbc = jdbc;
        this.messages = messages;
        this.presence = presence;
    }

    @PostMapping("/v1/users")
    public ResponseEntity<Void> user(@RequestBody NewUser u) {
        jdbc.sql("INSERT INTO users (id, name, presence_hidden) VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING")
                .params(u.id(), u.name(), u.presenceHidden()).update();
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @PostMapping("/v1/conversations")
    @Transactional
    public ResponseEntity<Void> conversation(@RequestBody NewConversation c) {
        if ("DIRECT".equals(c.kind()) && c.members().size() != 2) {
            throw new ChatException("BAD_REQUEST", "birebir sohbetin tam 2 üyesi olur");
        }
        if ("GROUP".equals(c.kind()) && c.members().size() > 500) {
            throw new ChatException("BAD_REQUEST", "grup en fazla 500 kişi");
        }
        jdbc.sql("INSERT INTO conversations (id, kind, title) VALUES (?, ?, ?)").params(c.id(), c.kind(), c.title()).update();
        for (Member m : c.members()) {
            jdbc.sql("INSERT INTO conversation_members (conversation_id, user_id, role) VALUES (?, ?, ?)")
                    .params(c.id(), m.userId(), m.role() == null ? "MEMBER" : m.role()).update();
        }
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/v1/conversations/{id}/messages")
    public List<MessageView> history(@RequestHeader("X-User-Id") long userId, @PathVariable long id,
                                     @RequestParam(defaultValue = "9223372036854775807") long before,
                                     @RequestParam(defaultValue = "50") int limit) {
        return messages.history(userId, id, before, limit);
    }

    @GetMapping("/v1/presence")
    public Map<String, Object> presence(@RequestParam List<Long> ids) {
        return presence.query(ids);
    }

    @ExceptionHandler(ChatException.class)
    public ResponseEntity<Map<String, String>> error(ChatException e) {
        HttpStatus s = switch (e.code()) {
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "FORBIDDEN" -> HttpStatus.FORBIDDEN;
            case "BUSY" -> HttpStatus.SERVICE_UNAVAILABLE;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(s).body(Map.of("code", e.code(), "message", e.getMessage()));
    }
}
