package io.paramix.llmgw.core;

import org.springframework.http.HttpStatus;

/** OpenAI-uyumlu hata gövdesine çevrilen gateway hatası. */
public class GatewayException extends RuntimeException {

    private final HttpStatus status;
    private final String code;
    private final Long retryAfterSeconds;

    public GatewayException(HttpStatus status, String code, String message) {
        this(status, code, message, null);
    }

    public GatewayException(HttpStatus status, String code, String message, Long retryAfterSeconds) {
        super(message);
        this.status = status;
        this.code = code;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }

    public Long retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
