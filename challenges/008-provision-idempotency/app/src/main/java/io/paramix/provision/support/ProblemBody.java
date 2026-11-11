package io.paramix.provision.support;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * RFC 9457 (problem+json) gövdesi. Hem anlık hatalarda hem de idempotency kaydına yazılıp aynen tekrar oynatılan
 * iş hatalarında aynı şekil kullanılır; istemci ikisini ayırt etmek zorunda kalmaz.
 *
 * @param code istemcinin dallanacağı makine-okur kod (ör. INSUFFICIENT_FUNDS). {@code title} insan için.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ProblemBody(String type, String title, int status, String code, String detail,
                          Map<String, Object> context) {

    public static ProblemBody of(int status, String code, String title, String detail, Map<String, Object> context) {
        return new ProblemBody("https://errors.paramix.io/" + code.toLowerCase().replace('_', '-'),
                title, status, code, detail, context);
    }
}
