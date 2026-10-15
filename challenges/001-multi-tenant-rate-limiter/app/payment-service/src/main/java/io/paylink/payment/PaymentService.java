package io.paylink.payment;

import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;

/**
 * Demo servis: veriyi bellekte tutar ama DB gecikmesini simüle eder.
 * "DB'ye giden" metodlar bulkhead ile korunur → aynı anda en fazla N çağrı.
 */
@Service
public class PaymentService {

    private final Map<String, PaymentLink> store = new ConcurrentHashMap<>();
    private final PaymentProperties props;

    public PaymentService(PaymentProperties props) {
        this.props = props;
    }

    @Bulkhead(name = "paymentDb")
    public PaymentLink create(String tenantId, CreatePaymentRequest req) {
        simulateDb();
        String id = "pl_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        PaymentLink link = new PaymentLink(id, tenantId, req.amount(), req.currency(), req.description(),
                props.publicLinkBaseUrl() + id, PaymentLink.Status.PENDING, Instant.now());
        store.put(id, link);
        return link;
    }

    @Bulkhead(name = "paymentDb")
    public Optional<PaymentLink> find(String tenantId, String id) {
        simulateDb();
        // Çok kiracılı izolasyon: başka tenant'ın linki "yok" gibi davranır.
        return Optional.ofNullable(store.get(id)).filter(l -> l.tenantId().equals(tenantId));
    }

    private void simulateDb() {
        try {
            Thread.sleep(props.simulatedDbLatency());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }
}
