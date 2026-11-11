package io.paramix.provision.idempotency;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * İstemciye gidecek cevap. Gövde, kayıttaki metnin kendisi: ilk cevap ile tekrar oynatılan cevap bayt bayt aynı.
 *
 * @param replayed true ise işlem bu istekte YAPILMADI, önceki sonucu döndürüyoruz ({@code Idempotent-Replayed: true}).
 */
public record IdempotentResponse(int status, String body, boolean replayed) {

    public static final String REPLAYED_HEADER = "Idempotent-Replayed";

    public ResponseEntity<String> toResponseEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(status >= 400 ? MediaType.APPLICATION_PROBLEM_JSON : MediaType.APPLICATION_JSON);
        headers.set(REPLAYED_HEADER, Boolean.toString(replayed));
        return ResponseEntity.status(status).headers(headers).body(body);
    }
}
