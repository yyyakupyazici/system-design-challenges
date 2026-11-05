package io.paramix.llmgw.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Gateway'in bütün ayarı: istemciler (API anahtarları), ekip bütçeleri, modeller ve fiyatları,
 * sağlayıcılar, yönlendirme ve cache. Başlangıçta doğrulanır; tutarsız config ile uygulama açılmaz.
 */
@ConfigurationProperties("gateway")
public record GatewayProperties(
        Map<String, ClientProperties> clients,
        Map<String, TeamProperties> teams,
        Map<String, ModelProperties> models,
        Map<String, ProviderProperties> providers,
        @DefaultValue RoutingProperties routing,
        @DefaultValue CacheProperties cache,
        @DefaultValue LimitProperties limits,
        @DefaultValue EmbeddingProperties embedding) {

    public enum AppType { CHAT, BATCH }

    public enum OverBudget { REJECT, DOWNGRADE }

    public enum Tier { SMALL, LARGE, INTERNAL }

    public enum ProviderType { FAKE, ANTHROPIC, OPENAI, OLLAMA }

    /**
     * Bir API anahtarının sahibi. Anahtarın kendisi değil SHA-256 özeti tutulur.
     *
     * @param appType CHAT = sohbet (prompt caching kârlı), BATCH = tek atımlık toplu iş (cache yazdırma zarar)
     * @param cacheable uygulama yanıt cache'ine izin veriyor mu (kişiye özel cevap üreten uygulamalar false)
     */
    public record ClientProperties(String keySha256, String team, String app,
                                   @DefaultValue("CHAT") AppType appType,
                                   @DefaultValue("true") boolean cacheable) {
    }

    /**
     * Günlük kredi bütçesi: üyelere garanti + paylaşılan havuz + kişi başı tavan (004'teki öncelikli kotanın aynısı).
     */
    public record TeamProperties(long dailyCredits,
                                 @DefaultValue("REJECT") OverBudget overBudget,
                                 @DefaultValue("200000") long tokensPerMinute,
                                 List<String> members,
                                 @DefaultValue("0") long guaranteePerMemberCredits,
                                 long userCeilingCredits) {

        public TeamProperties {
            members = members == null ? List.of() : List.copyOf(members);
            if (dailyCredits <= 0) {
                throw new IllegalArgumentException("dailyCredits > 0 olmalı");
            }
            long guaranteed = guaranteePerMemberCredits * members.size();
            if (guaranteed > dailyCredits) {
                throw new IllegalArgumentException("üye garantilerinin toplamı (" + guaranteed + ") günlük bütçeyi aşıyor");
            }
            if (userCeilingCredits <= 0) {
                userCeilingCredits = dailyCredits;
            }
            if (userCeilingCredits < guaranteePerMemberCredits) {
                throw new IllegalArgumentException("kişi başı tavan garantiden küçük olamaz");
            }
            if (userCeilingCredits > guaranteePerMemberCredits + (dailyCredits - guaranteed)) {
                throw new IllegalArgumentException("kişi başı tavan > garanti + paylaşılan havuz");
            }
        }

        public long poolCredits() {
            return dailyCredits - guaranteePerMemberCredits * members.size();
        }
    }

    /**
     * Fiyatlar 1M token başına kredi (IDE'deki fiyat listesiyle aynı dört kalem).
     *
     * @param version modelin sürümü; semantic cache bu sürümle kapsamlanır (sürüm değişince eski cevaplar kullanılmaz)
     */
    public record ModelProperties(String provider, String providerModel, Tier tier,
                                  @DefaultValue("v1") String version,
                                  long inputPrice, long outputPrice,
                                  @DefaultValue("0") long cacheReadPrice, @DefaultValue("0") long cacheWritePrice,
                                  @DefaultValue("1024") int defaultMaxTokens,
                                  @DefaultValue("4096") int maxOutputTokens,
                                  @DefaultValue("128000") int contextWindow,
                                  List<String> fallbacks) {

        public ModelProperties {
            fallbacks = fallbacks == null ? List.of() : List.copyOf(fallbacks);
        }
    }

    /**
     * @param internal kendi altyapımızda mı (kendi GPU kümemiz)? "gizli" veri sadece internal sağlayıcılara gider.
     * @param tokensPerMinute sağlayıcının kapasitesi; fallback'te yedeğin dolup taşmaması için (zorluk #2)
     */
    public record ProviderProperties(ProviderType type, String baseUrl, String apiKey,
                                     @DefaultValue("false") boolean internal,
                                     @DefaultValue("1000000") long tokensPerMinute) {
    }

    /** model = "auto" kararı. */
    public record RoutingProperties(@DefaultValue("small") String smallModel,
                                    @DefaultValue("large") String largeModel,
                                    @DefaultValue("internal") String confidentialModel,
                                    @DefaultValue("3000") int largeInputTokens,
                                    @DefaultValue({"analiz", "kod", "karşılaştır", "neden", "adım adım", "strateji",
                                            "analyze", "code", "compare", "why", "step by step"}) List<String> largeKeywords,
                                    @DefaultValue("10s") Duration firstTokenTimeout) {
    }

    public record CacheProperties(@DefaultValue("24h") Duration exactTtl,
                                  @DefaultValue("true") boolean semanticEnabled,
                                  @DefaultValue("0.92") double semanticThreshold,
                                  @DefaultValue("memory") String semanticStore,
                                  @DefaultValue("1024") int promptCacheMinPrefixTokens,
                                  // Kişinin VERİSİNİ isteyen ifadeler. İyelik eki tek başına yetmez:
                                  // "Kartımı nasıl iptal ederim?" genel bir prosedür sorusudur, cache'lenebilir.
                                  @DefaultValue({"hesabımda", "bakiyem", "bakiye", "harcamalarım", "işlemlerim", "borcum",
                                          "limitim", "ekstrem", "faturam", "my balance", "my transactions",
                                          "my statement", "in my account"}) List<String> personalMarkers,
                                  @DefaultValue({"gönder", "transfer", "öde", "havale yap", "iptal et", "send", "pay "}) List<String> actionMarkers) {
    }

    /**
     * @param reservationTtl bu sürede settle edilmeyen rezervasyon reaper tarafından kapatılır (pod öldü)
     */
    public record LimitProperties(@DefaultValue("32000") int maxInputTokens,
                                  @DefaultValue("10m") Duration reservationTtl,
                                  @DefaultValue("Europe/Istanbul") String budgetTimeZone) {
    }

    /** Semantic cache için embedding modeli: FAKE (test/lokal), OPENAI, OLLAMA. */
    public record EmbeddingProperties(@DefaultValue("FAKE") ProviderType type, String baseUrl, String apiKey,
                                      @DefaultValue("text-embedding-3-small") String model,
                                      @DefaultValue("256") int dimensions) {
    }

    public GatewayProperties {
        clients = clients == null ? Map.of() : Map.copyOf(clients);
        teams = teams == null ? Map.of() : Map.copyOf(teams);
        models = models == null ? Map.of() : Map.copyOf(models);
        providers = providers == null ? Map.of() : Map.copyOf(providers);
        for (Map.Entry<String, ModelProperties> m : models.entrySet()) {
            if (!providers.containsKey(m.getValue().provider())) {
                throw new IllegalArgumentException("model " + m.getKey() + " bilinmeyen sağlayıcıya bağlı: " + m.getValue().provider());
            }
            for (String fb : m.getValue().fallbacks()) {
                if (!models.containsKey(fb)) {
                    throw new IllegalArgumentException("model " + m.getKey() + " fallback'i tanımsız: " + fb);
                }
            }
        }
        for (Map.Entry<String, ClientProperties> c : clients.entrySet()) {
            if (!teams.containsKey(c.getValue().team())) {
                throw new IllegalArgumentException("istemci " + c.getKey() + " tanımsız ekibe bağlı: " + c.getValue().team());
            }
        }
    }
}
