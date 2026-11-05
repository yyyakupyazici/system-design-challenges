package io.paramix.llmgw.provider;

import reactor.core.publisher.Flux;

/** Bir sağlayıcının adaptörü. Gateway'in geri kalanı sağlayıcıya özgü hiçbir şey bilmez. */
public interface ProviderClient {

    String name();

    /** Kendi altyapımızda mı? "Gizli" veri sadece buraya gider. */
    boolean internal();

    ProviderReply call(ProviderRequest request);

    Flux<ProviderReply> stream(ProviderRequest request);
}
