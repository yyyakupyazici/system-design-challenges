package io.paramix.provision.idempotency;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * Doğrulanmış Idempotency-Key.
 *
 * @param value      istemcinin gönderdiği UUIDv7
 * @param issuedAt   key'in içindeki zaman (istemci saatine göre)
 * @param partitionDay kaydın yaşadığı günlük partition. issuedAt'ten türetildiği için aynı key hep aynı gün.
 */
public record IdempotencyKey(UUID value, Instant issuedAt, LocalDate partitionDay) {
}
