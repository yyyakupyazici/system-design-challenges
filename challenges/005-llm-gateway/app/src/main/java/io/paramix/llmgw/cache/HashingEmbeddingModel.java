package io.paramix.llmgw.cache;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;

/**
 * Anahtarsız, deterministik embedding (karakter trigram'larının hash'i): testler ve lokal demo için.
 * Gerçek anlam benzerliği yakalamaz (sadece yazım benzerliği); prod'da OpenAI/Ollama embedding modeli kullanılır.
 */
public class HashingEmbeddingModel implements EmbeddingModel {

    private final int dims;

    public HashingEmbeddingModel(int dims) {
        this.dims = dims;
    }

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) {
        List<Embedding> out = new ArrayList<>();
        List<String> inputs = request.getInstructions();
        for (int i = 0; i < inputs.size(); i++) {
            out.add(new Embedding(vector(inputs.get(i)), i));
        }
        return new EmbeddingResponse(out);
    }

    @Override
    public float[] embed(Document document) {
        return vector(document.getText());
    }

    @Override
    public int dimensions() {
        return dims;
    }

    float[] vector(String text) {
        float[] v = new float[dims];
        String t = Normalizer.normalize(text == null ? "" : text.toLowerCase(new Locale("tr")), Normalizer.Form.NFKC)
                .replaceAll("[^\\p{L}\\p{N} ]", " ").replaceAll("\\s+", " ").trim();
        String padded = " " + t + " ";
        for (int i = 0; i + 3 <= padded.length(); i++) {
            int h = padded.substring(i, i + 3).hashCode();
            v[Math.floorMod(h, dims)] += 1f;
        }
        double norm = 0;
        for (float f : v) {
            norm += f * f;
        }
        norm = Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < dims; i++) {
                v[i] /= (float) norm;
            }
        }
        return v;
    }
}
