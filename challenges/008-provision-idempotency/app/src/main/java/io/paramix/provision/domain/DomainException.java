package io.paramix.provision.domain;

import java.util.LinkedHashMap;
import java.util.Map;

import io.paramix.provision.support.ProblemBody;

public class DomainException extends RuntimeException {

    private final ErrorCode code;
    private final Map<String, Object> context;

    public DomainException(ErrorCode code, String detail) {
        this(code, detail, Map.of());
    }

    public DomainException(ErrorCode code, String detail, Map<String, Object> context) {
        super(detail, null, false, false);   // iş akışı istisnası: stack trace üretmeye değmez
        this.code = code;
        this.context = new LinkedHashMap<>(context);
    }

    public ErrorCode code() {
        return code;
    }

    public ProblemBody toProblem() {
        return ProblemBody.of(code.status(), code.name(), code.title(), getMessage(), context);
    }
}
