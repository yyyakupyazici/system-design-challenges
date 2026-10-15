package io.paylink.payment;

import io.github.resilience4j.bulkhead.BulkheadFullException;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Gateway arkasındaki iç API. Tenant kimliğini client'tan değil gateway'in eklediği
 * X-Tenant-Id header'ından alır (NetworkPolicy ile sadece gateway erişebilir).
 */
@RestController
@RequestMapping("/payments")
public class PaymentController {

    static final String TENANT_HEADER = "X-Tenant-Id";

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<PaymentLink> create(@RequestHeader(TENANT_HEADER) String tenantId,
                                              @Valid @RequestBody CreatePaymentRequest request) {
        PaymentLink link = service.create(tenantId, request);
        return ResponseEntity.created(URI.create("/payments/" + link.id())).body(link);
    }

    @GetMapping("/{id}")
    public ResponseEntity<PaymentLink> get(@RequestHeader(TENANT_HEADER) String tenantId, @PathVariable String id) {
        return ResponseEntity.of(service.find(tenantId, id));
    }

    /** Bulkhead dolu → hemen 503 (bekletip thread/connection tüketmek yok). */
    @ExceptionHandler(BulkheadFullException.class)
    public ResponseEntity<Map<String, String>> overloaded() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(Map.of("error", "service_overloaded"));
    }
}
