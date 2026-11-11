package io.paramix.provision.api;

import java.util.function.Supplier;

import io.paramix.provision.idempotency.CommandResult;
import io.paramix.provision.idempotency.IdempotencyKeyPolicy;
import io.paramix.provision.idempotency.IdempotentExecutor;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

/** Controller'ların idempotency'yi tek satırda kullanması için ince bir yardımcı. */
@Component
public class IdempotentEndpoint {

    private final IdempotencyKeyPolicy policy;
    private final IdempotentExecutor executor;

    public IdempotentEndpoint(IdempotencyKeyPolicy policy, IdempotentExecutor executor) {
        this.policy = policy;
        this.executor = executor;
    }

    /**
     * @param operation işlemin kalıcı adı (ör. "provision.create"). Parmak izine girer: aynı key başka bir işlemde
     *                  kullanılamaz. Değiştirmek, uçuştaki tekrar denemeleri 422'ye düşürür; sabit tutulmalı.
     * @param command   isteğin anlamını taşıyan, normalize edilmiş komut (path parametreleri dahil).
     */
    public ResponseEntity<String> handle(long customerId, String rawKey, String operation, Object command,
                                         Supplier<CommandResult> action) {
        var key = policy.parse(rawKey);
        return executor.execute(new IdempotentExecutor.Request(customerId, key, operation, command), action)
                .toResponseEntity();
    }
}
