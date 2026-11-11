package io.paramix.payreq.api;

import java.util.List;
import java.util.Map;

import io.paramix.payreq.domain.PaymentException;
import io.paramix.payreq.domain.PaymentRequest;
import io.paramix.payreq.domain.PaymentRequestRepository;
import io.paramix.payreq.domain.PaymentRequestService;
import io.paramix.payreq.domain.PaymentRequestService.CreateCommand;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * İstemci (mobil uygulama) bu servise DOĞRUDAN gelir; sohbetin WebSocket'inden geçmez. Para hareketi sohbet
 * sunucusunun uptime'ına ve deploy'una bağlı değil. Kimlik demo amaçlı X-User-Id (üretimde JWT).
 *
 * <pre>
 *  POST /v1/payment-requests            (Idempotency-Key)  {payerId, amountKurus, description, origin: {kind, ref}}
 *  POST /v1/payment-requests/{id}/pay
 *  POST /v1/payment-requests/{id}/decline
 *  GET  /v1/payment-requests/{id}
 *  GET  /v1/payment-requests/pending                       bana gelen bekleyen istekler
 * </pre>
 */
@RestController
public class PaymentRequestController {

    public record Origin(String kind, String ref) {
    }

    public record CreateBody(long payerId, long amountKurus, String description, Origin origin) {
    }

    private final PaymentRequestService service;
    private final PaymentRequestRepository repo;

    public PaymentRequestController(PaymentRequestService service, PaymentRequestRepository repo) {
        this.service = service;
        this.repo = repo;
    }

    @PostMapping("/v1/payment-requests")
    public ResponseEntity<PaymentRequest> create(@RequestHeader("X-User-Id") long userId,
                                                 @RequestHeader("Idempotency-Key") String key,
                                                 @RequestBody CreateBody b) {
        Origin o = b.origin() == null ? new Origin("APP", null) : b.origin();
        return ResponseEntity.status(HttpStatus.CREATED).body(service.create(new CreateCommand(userId, b.payerId(),
                b.amountKurus(), b.description(), o.kind(), o.ref(), key)));
    }

    @PostMapping("/v1/payment-requests/{id}/pay")
    public Map<String, Object> pay(@RequestHeader("X-User-Id") long userId, @PathVariable long id) {
        return Map.of("paymentRequestId", id, "status", service.pay(userId, id));
    }

    @PostMapping("/v1/payment-requests/{id}/decline")
    public Map<String, Object> decline(@RequestHeader("X-User-Id") long userId, @PathVariable long id) {
        return Map.of("paymentRequestId", id, "declined", service.decline(userId, id));
    }

    @GetMapping("/v1/payment-requests/{id}")
    public PaymentRequest get(@RequestHeader("X-User-Id") long userId, @PathVariable long id) {
        return service.get(userId, id);
    }

    @GetMapping("/v1/payment-requests/pending")
    public List<PaymentRequest> pending(@RequestHeader("X-User-Id") long userId) {
        return repo.pendingFor(userId);
    }

    @ExceptionHandler(PaymentException.class)
    public ResponseEntity<Map<String, String>> error(PaymentException e) {
        HttpStatus s = switch (e.code()) {
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "FORBIDDEN" -> HttpStatus.FORBIDDEN;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(s).body(Map.of("code", e.code(), "message", e.getMessage()));
    }
}
