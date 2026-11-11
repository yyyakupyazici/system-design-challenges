package io.paramix.provision.idempotency;

import java.util.UUID;

import io.paramix.provision.domain.DomainException;

/**
 * Bir komutun kalıcı sonucu. İş hatası da bir sonuçtur: "bakiye yetersiz" cevabı da kaydedilir ve aynı key ile
 * tekrar sorulduğunda aynen döner. (Kullanıcı arada para yatırmış olsa bile: o key o denemeyi temsil eder;
 * yeni deneme yeni key ister. Stripe ve benzeri ödeme API'leri de böyle çalışır.)
 */
public record CommandResult(int status, Object body, UUID resourceId) {

    public static CommandResult created(Object body, UUID resourceId) {
        return new CommandResult(201, body, resourceId);
    }

    public static CommandResult accepted(Object body, UUID resourceId) {
        return new CommandResult(202, body, resourceId);
    }

    public static CommandResult ok(Object body, UUID resourceId) {
        return new CommandResult(200, body, resourceId);
    }

    public static CommandResult failure(DomainException e) {
        return new CommandResult(e.code().status(), e.toProblem(), null);
    }
}
