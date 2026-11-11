package io.paramix.provision.idempotency;

import java.security.MessageDigest;
import java.util.Map;
import java.util.function.Supplier;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.domain.DomainException;
import io.paramix.provision.idempotency.IdempotencyRepository.StoredRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * API idempotency'nin tek giriş noktası.
 *
 * <pre>
 *  BEGIN
 *    ① KAP      INSERT idempotency_keys ... ON CONFLICT DO NOTHING      (eşzamanlı kopyalar burada sıraya girer)
 *               └─ kapılamadı → kaydı oku → parmak izi farklı mı? 422 : kayıtlı cevabı aynen dön
 *    ② İŞ       SAVEPOINT; komut (provizyon, bakiye, ledger, outbox)
 *               └─ iş hatası (bakiye yetersiz...) → ROLLBACK TO SAVEPOINT; hata da bir sonuçtur
 *    ③ SONUÇ    UPDATE idempotency_keys SET response = ...
 *  COMMIT                                                               (①②③ ya hep ya hiç)
 * </pre>
 *
 * <b>Neden hepsi tek transaction:</b> idempotency kaydı ile para hareketi ayrı yerlerde (ör. Redis + PostgreSQL)
 * ya da ayrı transaction'larda olsaydı, ikisinin arasındaki her çökme ya "para gitti, kayıt yok" (tekrar denemede
 * ikinci kez para gider) ya da "kayıt var, para gitmedi" (işlem sessizce kaybolur) bırakırdı. Tek transaction'da
 * çökme = ROLLBACK = hiçbir şey olmamış; istemci aynı key ile tekrar dener ve işlem bir kez yapılır.
 *
 * <b>Neden SAVEPOINT:</b> iş hatası, komutun yarıda bıraktığı yazmaları geri almalı ama key'in kaydını değil.
 * Beklenmeyen hatalar (veritabanı koptu, bug) ise yakalanmaz: bütün transaction geri alınır, kayıt kalmaz ve
 * istemci aynı key ile güvenle tekrar dener.
 *
 * <b>Neden IN_PROGRESS durumu yok:</b> satır commit'e kadar başkasına görünmez, commit anında da cevap doludur.
 * "Takılı kalmış IN_PROGRESS" diye bir durum bu tasarımda oluşamaz; kilidi veritabanı tutar, sahibi ölünce bırakır.
 * Dış sistem çağrısı gereken çok adımlı işler (zincire gönderim) istek yolunda yapılmaz, bkz. WithdrawalWorker.
 */
@Component
public class IdempotentExecutor {

    public record Request(long customerId, IdempotencyKey key, String operation, Object command) {
    }

    private final IdempotencyRepository repo;
    private final RequestFingerprint fingerprint;
    private final ObjectMapper json;
    private final TransactionTemplate tx;
    private final TransactionTemplate savepoint;
    private final long lockWaitMillis;
    private final MeterRegistry meters;

    public IdempotentExecutor(IdempotencyRepository repo, RequestFingerprint fingerprint, ObjectMapper json,
                              PlatformTransactionManager txManager, ProvisionProperties props, MeterRegistry meters) {
        this.repo = repo;
        this.fingerprint = fingerprint;
        this.json = json;
        this.tx = new TransactionTemplate(txManager);
        this.savepoint = new TransactionTemplate(txManager);
        this.savepoint.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        this.lockWaitMillis = props.idempotency().lockWaitTimeout().toMillis();
        this.meters = meters;
    }

    public IdempotentResponse execute(Request request, Supplier<CommandResult> command) {
        byte[] hash = fingerprint.of(request.operation(), request.command());
        try {
            IdempotentResponse response = tx.execute(status -> {
                if (!repo.tryClaim(request.customerId(), request.key(), request.operation(), hash, lockWaitMillis)) {
                    return replay(request, hash);
                }
                CommandResult result = runInSavepoint(command);
                String body = write(result.body());
                repo.complete(request.customerId(), request.key(), result.status(), body, result.resourceId());
                return new IdempotentResponse(result.status(), body, false);
            });
            count(request.operation(), response.replayed() ? "replayed" : "executed");
            return response;
        } catch (IdempotencyException e) {
            count(request.operation(), e.reason().name().toLowerCase());
            throw e;
        }
    }

    private IdempotentResponse replay(Request request, byte[] hash) {
        StoredRequest stored = repo.find(request.customerId(), request.key())
                .orElseThrow(() -> new IllegalStateException("Çakışan idempotency kaydı okunamadı: " + request.key().value()));
        if (!MessageDigest.isEqual(stored.requestHash(), hash)) {
            // Sessizce eski cevabı dönmek en tehlikeli hata olurdu: "100 USDT çek" key'iyle gelen
            // "10.000 USDT çek" isteğine "tamam" demek. Ne yapıldığını değil, neyin yapılmadığını açıkça söyle.
            throw new IdempotencyException(IdempotencyException.Reason.KEY_REUSED,
                    "Bu key daha önce farklı içerikli bir istek için kullanıldı; yeni istek için yeni key üretin",
                    Map.of("originalOperation", stored.operation()));
        }
        return new IdempotentResponse(stored.responseStatus(), stored.responseBody(), true);
    }

    private CommandResult runInSavepoint(Supplier<CommandResult> command) {
        try {
            return savepoint.execute(s -> command.get());
        } catch (DomainException e) {
            return CommandResult.failure(e);
        }
    }

    private String write(Object body) {
        try {
            return json.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cevap serileştirilemedi", e);
        }
    }

    private void count(String operation, String outcome) {
        meters.counter("idempotency.requests", "operation", operation, "outcome", outcome).increment();
    }
}
