package io.paramix.llmgw.config;

import io.paramix.llmgw.cache.HashingEmbeddingModel;
import io.paramix.llmgw.config.GatewayProperties.EmbeddingProperties;

import org.springframework.ai.document.MetadataMode;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.ollama.OllamaEmbeddingModel;
import org.springframework.ai.ollama.api.OllamaApi;
import org.springframework.ai.ollama.api.OllamaEmbeddingOptions;
import org.springframework.ai.openai.OpenAiEmbeddingModel;
import org.springframework.ai.openai.OpenAiEmbeddingOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.ai.vectorstore.SimpleVectorStore;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.pgvector.PgVectorStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Semantic cache'in embedding modeli ve vektör deposu. Sohbet modelleri burada değil, {@code ProviderRegistry}'de:
 * onlar config'teki sağlayıcı listesinden dinamik kurulur.
 */
@Configuration
public class AiConfig {

    @Bean
    EmbeddingModel embeddingModel(GatewayProperties props) {
        EmbeddingProperties e = props.embedding();
        return switch (e.type()) {
            case FAKE -> new HashingEmbeddingModel(e.dimensions());
            case OPENAI -> {
                OpenAiApi.Builder api = OpenAiApi.builder().apiKey(e.apiKey());
                if (e.baseUrl() != null && !e.baseUrl().isBlank()) {
                    api.baseUrl(e.baseUrl());
                }
                yield new OpenAiEmbeddingModel(api.build(), MetadataMode.EMBED,
                        OpenAiEmbeddingOptions.builder().model(e.model()).dimensions(e.dimensions()).build());
            }
            case OLLAMA -> OllamaEmbeddingModel.builder()
                    .ollamaApi(OllamaApi.builder().baseUrl(e.baseUrl()).build())
                    .defaultOptions(OllamaEmbeddingOptions.builder().model(e.model()).build())
                    .build();
            case ANTHROPIC -> throw new IllegalArgumentException("Anthropic embedding modeli sunmuyor; OPENAI/OLLAMA seçin");
        };
    }

    @Bean
    VectorStore semanticVectorStore(GatewayProperties props, EmbeddingModel embeddingModel, JdbcTemplate jdbc) {
        if ("pgvector".equalsIgnoreCase(props.cache().semanticStore())) {
            return PgVectorStore.builder(jdbc, embeddingModel)
                    .dimensions(props.embedding().dimensions())
                    .initializeSchema(true)
                    .build();
        }
        return SimpleVectorStore.builder(embeddingModel).build();
    }
}
