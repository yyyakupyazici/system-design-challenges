package io.paramix.llmgw.api;

import java.util.Map;

import io.paramix.llmgw.core.GatewayException;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** OpenAI tarzı hata gövdesi: {@code {"error": {"type", "code", "message"}}}; 429'da {@code Retry-After}. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(GatewayException.class)
    public ResponseEntity<Map<String, Object>> gateway(GatewayException e) {
        ResponseEntity.BodyBuilder b = ResponseEntity.status(e.status());
        if (e.retryAfterSeconds() != null) {
            b.header(HttpHeaders.RETRY_AFTER, String.valueOf(e.retryAfterSeconds()));
        }
        return b.body(body(e.status(), e.code(), e.getMessage()));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<Map<String, Object>> invalid(Exception e) {
        String msg = e instanceof MethodArgumentNotValidException m && m.getBindingResult().getFieldError() != null
                ? m.getBindingResult().getFieldError().getField() + ": " + m.getBindingResult().getFieldError().getDefaultMessage()
                : "Geçersiz istek gövdesi";
        return ResponseEntity.badRequest().body(body(HttpStatus.BAD_REQUEST, "invalid_request", msg));
    }

    private static Map<String, Object> body(HttpStatus status, String code, String message) {
        String type = switch (status) {
            case UNAUTHORIZED -> "authentication_error";
            case FORBIDDEN -> "permission_error";
            case TOO_MANY_REQUESTS -> "rate_limit_error";
            case PAYMENT_REQUIRED -> "insufficient_quota";
            case BAD_REQUEST, PAYLOAD_TOO_LARGE -> "invalid_request_error";
            default -> "api_error";
        };
        return Map.of("error", Map.of("type", type, "code", code, "message", message == null ? "" : message));
    }
}
