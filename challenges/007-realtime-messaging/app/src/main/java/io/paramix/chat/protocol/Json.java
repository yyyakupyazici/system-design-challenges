package io.paramix.chat.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class Json {

    private final ObjectMapper mapper;

    public Json(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String write(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public JsonNode read(String s) {
        try {
            return mapper.readTree(s);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("geçersiz JSON", e);
        }
    }

    public ObjectMapper mapper() {
        return mapper;
    }
}
