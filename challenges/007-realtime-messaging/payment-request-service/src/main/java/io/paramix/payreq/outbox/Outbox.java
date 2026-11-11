package io.paramix.payreq.outbox;

import io.paramix.payreq.domain.PaymentRequestEvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Olayı çağıranın transaction'ında yazar: durum geçişi ile olay birlikte yazılır ya da hiçbiri. */
@Component
public class Outbox {

    private final JdbcClient jdbc;
    private final ObjectMapper json;

    public Outbox(JdbcClient jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(PaymentRequestEvent e) {
        try {
            jdbc.sql("INSERT INTO outbox (event_key, payload) VALUES (?, ?)")
                    .params(String.valueOf(e.paymentRequestId()), json.writeValueAsString(e)).update();
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
