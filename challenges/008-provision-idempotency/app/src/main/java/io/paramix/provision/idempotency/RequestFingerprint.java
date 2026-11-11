package io.paramix.provision.idempotency;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.springframework.stereotype.Component;

/**
 * İsteğin parmak izi: SHA-256(operation + kanonik komut).
 *
 * <p>Ham HTTP gövdesinin değil, <b>ayrıştırılmış komutun</b> izi alınır. Böylece anlamca aynı istekler aynı izi verir:
 * <ul>
 *   <li>alan sırası farkı ({@code {"a":1,"b":2}} ↔ {@code {"b":2,"a":1}}),</li>
 *   <li>boşluk farkı,</li>
 *   <li>tutar yazımı ({@code 100}, {@code 100.0}, {@code 100.00} aynı tutar).</li>
 * </ul>
 * Anlamca farklı istek (tutar, varlık, hedef adres...) farklı iz verir ve aynı key ile gelirse 422 alır.
 * Operation da ize dahil: bir provizyon key'i çekim isteğinde tekrar kullanılamaz.
 */
@Component
public class RequestFingerprint {

    private final ObjectMapper canonical = JsonMapper.builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .addModule(new SimpleModule().addSerializer(BigDecimal.class, new CanonicalDecimal()))
            .findAndAddModules()
            .build();

    public byte[] of(String operation, Object command) {
        try {
            byte[] body = canonical.writeValueAsBytes(command);
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update(operation.getBytes(StandardCharsets.UTF_8));
            sha.update((byte) '\n');
            sha.update(body);
            return sha.digest();
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Parmak izi hesaplanamadı", e);
        }
    }

    private static final class CanonicalDecimal extends StdSerializer<BigDecimal> {

        CanonicalDecimal() {
            super(BigDecimal.class);
        }

        @Override
        public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(value.signum() == 0 ? "0" : value.stripTrailingZeros().toPlainString());
        }
    }
}
