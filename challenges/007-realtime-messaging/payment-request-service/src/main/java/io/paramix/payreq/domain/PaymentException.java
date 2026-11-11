package io.paramix.payreq.domain;

public class PaymentException extends RuntimeException {

    private final String code;

    public PaymentException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
