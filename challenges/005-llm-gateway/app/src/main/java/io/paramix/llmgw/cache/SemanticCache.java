package io.paramix.llmgw.cache;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.paramix.llmgw.config.GatewayProperties.CacheProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Component;

/**
 * Anlamca benzer soru → kayıtlı cevap. Spring AI {@link VectorStore} (prod: pgvector).
 *
 * <p>Yanlış isabet (&lt; %0,5 hedefi) için üç savunma:
 * <ol>
 *   <li>Yüksek benzerlik eşiği (varsayılan 0,92).</li>
 *   <li>Kapsam: ekip + uygulama + model + model SÜRÜMÜ. Model sürümü değişince eski cevaplar hiç bulunmaz (zorluk #7).</li>
 *   <li>Sayı koruması: "500 TL" ile "600 TL" vektör olarak neredeyse aynıdır ama cevapları farklıdır;
 *       iki sorudaki sayılar birebir aynı değilse isabet sayılmaz (zorluk #3).</li>
 * </ol>
 */
@Component
public class SemanticCache {

    private static final Logger log = LoggerFactory.getLogger(SemanticCache.class);
    private static final Pattern NUMBER = Pattern.compile("\\d+(?:[.,]\\d+)*");

    private final VectorStore store;
    private final CacheProperties props;

    public SemanticCache(VectorStore store, io.paramix.llmgw.config.GatewayProperties gateway) {
        this.store = store;
        this.props = gateway.cache();
    }

    public static String scope(String team, String app, String modelAlias, String modelVersion) {
        return team + "|" + app + "|" + modelAlias + "|" + modelVersion;
    }

    public Optional<CachedAnswer> lookup(String scope, String question) {
        try {
            List<Document> hits = store.similaritySearch(SearchRequest.builder()
                    .query(question)
                    .topK(1)
                    .similarityThreshold(props.semanticThreshold())
                    .filterExpression("scope == '" + scope.replace("'", "") + "'")
                    .build());
            if (hits == null || hits.isEmpty()) {
                return Optional.empty();
            }
            Document d = hits.getFirst();
            if (!numbers(question).equals(numbers(d.getText()))) {
                log.debug("Semantic aday sayı farkı yüzünden reddedildi: '{}' vs '{}'", question, d.getText());
                return Optional.empty();
            }
            Map<String, Object> md = d.getMetadata();
            return Optional.of(new CachedAnswer((String) md.get("answer"), (String) md.get("model"),
                    ((Number) md.getOrDefault("promptTokens", 0)).intValue(),
                    ((Number) md.getOrDefault("completionTokens", 0)).intValue()));
        } catch (Exception e) {
            log.debug("Semantic cache aranamadı: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public void put(String scope, String question, CachedAnswer answer) {
        try {
            store.add(List.of(new Document(question, Map.of(
                    "scope", scope,
                    "answer", answer.content(),
                    "model", answer.model(),
                    "promptTokens", answer.promptTokens(),
                    "completionTokens", answer.completionTokens(),
                    "createdAt", Instant.now().toString()))));
        } catch (Exception e) {
            log.debug("Semantic cache'e yazılamadı: {}", e.getMessage());
        }
    }

    static Set<String> numbers(String text) {
        if (text == null) {
            return Set.of();
        }
        Matcher m = NUMBER.matcher(text);
        return m.results().map(r -> r.group().replace(".", "").replace(",", "")).collect(Collectors.toSet());
    }
}
