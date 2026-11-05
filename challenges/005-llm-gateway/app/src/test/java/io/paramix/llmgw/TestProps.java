package io.paramix.llmgw;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.paramix.llmgw.auth.ClientContext;
import io.paramix.llmgw.config.GatewayProperties;
import io.paramix.llmgw.config.GatewayProperties.AppType;
import io.paramix.llmgw.config.GatewayProperties.CacheProperties;
import io.paramix.llmgw.config.GatewayProperties.ModelProperties;
import io.paramix.llmgw.config.GatewayProperties.ProviderProperties;
import io.paramix.llmgw.config.GatewayProperties.ProviderType;
import io.paramix.llmgw.config.GatewayProperties.RoutingProperties;
import io.paramix.llmgw.config.GatewayProperties.Tier;

/** Birim testleri için elle kurulmuş config (Spring'siz). */
public final class TestProps {

    private TestProps() {
    }

    public static GatewayProperties props() {
        Map<String, ModelProperties> models = Map.of(
                "small", model("ext", Tier.SMALL, 20, 100, List.of("small-backup")),
                "small-backup", model("ext2", Tier.SMALL, 30, 120, List.of()),
                "large", model("ext", Tier.LARGE, 200, 1000, List.of("large-backup", "internal")),
                "large-backup", model("ext2", Tier.LARGE, 250, 1000, List.of()),
                "internal", model("gpu", Tier.INTERNAL, 8, 24, List.of()));
        Map<String, ProviderProperties> providers = Map.of(
                "ext", new ProviderProperties(ProviderType.FAKE, null, null, false, 1_000_000),
                "ext2", new ProviderProperties(ProviderType.FAKE, null, null, false, 1_000_000),
                "gpu", new ProviderProperties(ProviderType.FAKE, null, null, true, 1_000_000));
        return new GatewayProperties(Map.of(), Map.of(), models, providers, routing(), cache(), null, null);
    }

    static ModelProperties model(String provider, Tier tier, long in, long out, List<String> fallbacks) {
        return new ModelProperties(provider, provider + "-model", tier, "v1", in, out, 0, 0, 1024, 4096, 128000, fallbacks);
    }

    public static RoutingProperties routing() {
        return new RoutingProperties("small", "large", "internal", 3000,
                List.of("analiz", "kod", "karşılaştır", "neden", "adım adım"), Duration.ofSeconds(10));
    }

    public static CacheProperties cache() {
        return new CacheProperties(Duration.ofHours(24), true, 0.92, "memory", 1024,
                List.of("hesabımda", "bakiyem", "bakiye", "harcamalarım", "işlemlerim", "borcum", "limitim", "ekstrem",
                        "faturam", "my balance"),
                List.of("gönder", "transfer", "öde", "havale yap", "iptal et", "send", "pay "));
    }

    public static ClientContext chat(boolean confidential, String session) {
        return new ClientContext("support", "bot", AppType.CHAT, true, "u1", confidential, session);
    }
}
