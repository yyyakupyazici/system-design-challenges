package io.paramix.llmgw.provider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import io.paramix.llmgw.pricing.TokenEstimator;
import io.paramix.llmgw.pricing.TokenUsage;

import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.metadata.ChatResponseMetadata;
import org.springframework.ai.chat.metadata.DefaultUsage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import reactor.core.publisher.Flux;

/**
 * Anahtarsız, deterministik Spring AI {@link ChatModel}: testler ve lokal demo için.
 *
 * <ul>
 *   <li>Cevap: {@code [model] <son kullanıcı mesajı>} yankısı (yer tutucular dahil; geri açma testlenebilir).</li>
 *   <li>Prompt caching taklidi: aynı "önek" (son kullanıcı mesajı hariç her şey) daha önce görüldüyse ve cache
 *       istendiyse o kısım cache okuma, yeni kısım cache yazma olarak bildirilir.</li>
 *   <li>Senaryo: sıradaki çağrıların sonucu ayarlanabilir (ilk token'dan önce hata, stream ortasında kopma, yavaşlık).</li>
 * </ul>
 */
public class FakeChatModel implements ChatModel {

    public enum Outcome { OK, FAIL_BEFORE_FIRST_TOKEN, FAIL_MID_STREAM, SLOW_FIRST_TOKEN }

    public record CallRecord(String model, List<String> messages, boolean promptCache) {
    }

    private final String name;
    private final Queue<Outcome> script = new ConcurrentLinkedQueue<>();
    private final Set<String> seenPrefixes = ConcurrentHashMap.newKeySet();
    private final List<CallRecord> calls = new CopyOnWriteArrayList<>();
    private final AtomicInteger callCount = new AtomicInteger();
    private volatile Duration slowDelay = Duration.ofSeconds(30);

    public FakeChatModel(String name) {
        this.name = name;
    }

    @Override
    public ChatResponse call(Prompt prompt) {
        Outcome o = next(prompt);
        if (o == Outcome.FAIL_BEFORE_FIRST_TOKEN) {
            throw new IllegalStateException(name + ": 529 overloaded");
        }
        String text = answer(prompt);
        return response(text, usage(prompt, text), "stop");
    }

    @Override
    public Flux<ChatResponse> stream(Prompt prompt) {
        Outcome o = next(prompt);
        if (o == Outcome.FAIL_BEFORE_FIRST_TOKEN) {
            return Flux.error(new IllegalStateException(name + ": 529 overloaded"));
        }
        String text = answer(prompt);
        List<String> parts = split(text, 7);
        TokenUsage usage = usage(prompt, text);
        List<ChatResponse> chunks = new ArrayList<>();
        for (String p : parts) {
            chunks.add(response(p, null, null));
        }
        chunks.add(response("", usage, "stop"));
        Flux<ChatResponse> flux = Flux.fromIterable(chunks);
        if (o == Outcome.FAIL_MID_STREAM) {
            flux = Flux.fromIterable(chunks.subList(0, Math.min(2, chunks.size())))
                    .concatWith(Flux.error(new IllegalStateException(name + ": connection reset")));
        }
        if (o == Outcome.SLOW_FIRST_TOKEN) {
            flux = flux.delaySubscription(slowDelay);
        }
        return flux;
    }

    private Outcome next(Prompt prompt) {
        callCount.incrementAndGet();
        boolean cache = prompt.getOptions() instanceof FakeChatOptions f && f.promptCache();
        String model = prompt.getOptions() == null ? name : prompt.getOptions().getModel();
        calls.add(new CallRecord(model, prompt.getInstructions().stream().map(Message::getText).toList(), cache));
        Outcome o = script.poll();
        return o == null ? Outcome.OK : o;
    }

    private String answer(Prompt prompt) {
        String model = prompt.getOptions() == null ? name : prompt.getOptions().getModel();
        List<Message> msgs = prompt.getInstructions();
        String last = msgs.isEmpty() ? "" : msgs.getLast().getText();
        return "[" + model + "] " + last;
    }

    /** Önek daha önce görüldüyse cache'ten okundu say; değilse (cache isteniyorsa) yazıldı say. */
    private TokenUsage usage(Prompt prompt, String answer) {
        List<Message> msgs = prompt.getInstructions();
        int total = TokenEstimator.estimateMessages(msgs.stream().map(Message::getText).toList());
        int out = TokenEstimator.estimate(answer);
        boolean cache = prompt.getOptions() instanceof FakeChatOptions f && f.promptCache();
        if (!cache || msgs.size() < 2) {
            return TokenUsage.of(total, out);
        }
        List<String> prefixTexts = msgs.subList(0, msgs.size() - 1).stream()
                .map(m -> m.getMessageType() + ":" + m.getText()).toList();
        int prefixTokens = TokenEstimator.estimateMessages(msgs.subList(0, msgs.size() - 1).stream().map(Message::getText).toList());
        int suffix = total - prefixTokens;
        // Bir önceki turun öneki bu turun öneğinin başlangıcıysa o kısım okunur.
        int read = 0;
        for (int k = prefixTexts.size() - 1; k >= 1; k--) {
            if (seenPrefixes.contains(String.join("\u0000", prefixTexts.subList(0, k)))) {
                read = TokenEstimator.estimateMessages(msgs.subList(0, k).stream().map(Message::getText).toList());
                break;
            }
        }
        seenPrefixes.add(String.join("\u0000", prefixTexts));
        int write = prefixTokens - read;
        return new TokenUsage(suffix, read, write, out);
    }

    private static ChatResponse response(String text, TokenUsage usage, String finish) {
        ChatResponseMetadata.Builder md = ChatResponseMetadata.builder().model("fake");
        if (usage != null) {
            md.usage(new DefaultUsage(usage.totalInput(), usage.output(), usage.totalInput() + usage.output(), usage));
        }
        return new ChatResponse(List.of(new Generation(new AssistantMessage(text))), md.build());
    }

    private static List<String> split(String text, int size) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < text.length(); i += size) {
            out.add(text.substring(i, Math.min(text.length(), i + size)));
        }
        return out;
    }

    // --- test kancaları ---

    public void script(Outcome... outcomes) {
        script.addAll(List.of(outcomes));
    }

    public void slowDelay(Duration d) {
        this.slowDelay = d;
    }

    public List<CallRecord> calls() {
        return List.copyOf(calls);
    }

    public int callCount() {
        return callCount.get();
    }

    public void reset() {
        script.clear();
        calls.clear();
        callCount.set(0);
        seenPrefixes.clear();
    }
}
