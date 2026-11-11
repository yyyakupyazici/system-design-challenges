package io.paramix.provision.api;

import java.util.Map;
import java.util.stream.Collectors;

import io.paramix.provision.domain.DomainException;
import io.paramix.provision.idempotency.IdempotencyException;
import io.paramix.provision.support.ProblemBody;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * Anlık hatalar (idempotency kaydına YAZILMAYANLAR). Hepsi aynı problem+json şeklinde.
 * 5xx/503 cevapları "tekrar dene" anlamına gelir: transaction geri alındı, key kaydı yok, aynı key ile güvenli.
 */
@RestControllerAdvice
public class ApiErrors {

    private static final Logger log = LoggerFactory.getLogger(ApiErrors.class);

    @ExceptionHandler(IdempotencyException.class)
    ResponseEntity<ProblemBody> idempotency(IdempotencyException e) {
        var r = e.reason();
        var response = ResponseEntity.status(r.status()).contentType(MediaType.APPLICATION_PROBLEM_JSON);
        if (r == IdempotencyException.Reason.REQUEST_IN_PROGRESS) {
            response.header(HttpHeaders.RETRY_AFTER, "1");
        }
        return response.body(ProblemBody.of(r.status(), r.code(), r.title(), e.getMessage(), e.context()));
    }

    /** Idempotent olmayan uçlardaki (GET, iç yatırım) iş hataları. */
    @ExceptionHandler(DomainException.class)
    ResponseEntity<ProblemBody> domain(DomainException e) {
        return problem(e.toProblem());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemBody> invalid(MethodArgumentNotValidException e) {
        Map<String, Object> fields = e.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.toMap(f -> f.getField(), f -> String.valueOf(f.getDefaultMessage()), (a, b) -> a));
        return problem(ProblemBody.of(400, "VALIDATION_FAILED", "İstek doğrulanamadı", null, fields));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class,
            MissingRequestHeaderException.class})
    ResponseEntity<ProblemBody> malformed(Exception e) {
        return problem(ProblemBody.of(400, "MALFORMED_REQUEST", "İstek okunamadı", e.getMessage(), Map.of()));
    }

    @ExceptionHandler({TransientDataAccessException.class, PessimisticLockingFailureException.class})
    ResponseEntity<ProblemBody> transientFailure(Exception e) {
        log.warn("Geçici veritabanı hatası: {}", e.toString());
        return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "1")
                .contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .body(ProblemBody.of(503, "TEMPORARILY_UNAVAILABLE", "Geçici hata",
                        "İşlem yapılmadı; aynı Idempotency-Key ile tekrar deneyin", Map.of()));
    }

    private static ResponseEntity<ProblemBody> problem(ProblemBody body) {
        return ResponseEntity.status(body.status()).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(body);
    }
}
