package io.paramix.notify.worker;

import java.util.List;
import java.util.Optional;

import io.paramix.notify.config.NotifyProperties;
import io.paramix.notify.domain.DeliveryMessage;
import io.paramix.notify.domain.DeliveryStatus;
import io.paramix.notify.domain.Priority;
import io.paramix.notify.provider.ChannelProvider;
import io.paramix.notify.provider.OutboundMessage;
import io.paramix.notify.provider.ProviderRegistry;
import io.paramix.notify.provider.SendResult;
import io.paramix.notify.quota.AimdController;
import io.paramix.notify.quota.QuotaDecision;
import io.paramix.notify.quota.QuotaService;
import io.paramix.notify.store.CampaignStore;
import io.paramix.notify.store.Delivery;
import io.paramix.notify.store.DeliveryRepository;
import io.paramix.notify.store.UserRepository;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Worker'ın tek işi: sahiplen → (pazarlamaysa son kontrol) → kota al → sağlayıcıya gönder → sonucu kaydet.
 *
 * <pre>
 *  claim ──yok──► atla (başka pod gönderiyor / zaten gönderildi)
 *    │
 *    ▼ sağlayıcı zinciri (birincil, yedek...)
 *  breaker açık? ──► sıradaki sağlayıcı
 *  acquire (öncelik bazlı bekleme) ──red──► sıradaki sağlayıcı
 *  send ──Accepted──► SENT
 *       ──Permanent─► FAILED
 *       ──429───────► AIMD düşür, sıradaki sağlayıcı
 *       ──Unavailable► sıradaki sağlayıcı (istek ulaşmadı, güvenli)
 *       ──Timeout───► UNKNOWN → reconcile (sağlayıcıya sor)
 *  hiçbiri olmadı ──► PENDING'e bırak, RetryLater (Kafka kaydı yeniden dener)
 * </pre>
 */
@Service
public class DeliveryProcessor {

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessor.class);

    private final DeliveryRepository deliveries;
    private final ProviderRegistry providers;
    private final QuotaService quota;
    private final AimdController aimd;
    private final Reconciler reconciler;
    private final CampaignStore campaigns;
    private final UserRepository users;
    private final NotifyProperties props;
    private final MeterRegistry meters;

    public DeliveryProcessor(DeliveryRepository deliveries, ProviderRegistry providers, QuotaService quota,
                             AimdController aimd, Reconciler reconciler, CampaignStore campaigns,
                             UserRepository users, NotifyProperties props, MeterRegistry meters) {
        this.deliveries = deliveries;
        this.providers = providers;
        this.quota = quota;
        this.aimd = aimd;
        this.reconciler = reconciler;
        this.campaigns = campaigns;
        this.users = users;
        this.props = props;
        this.meters = meters;
    }

    public void process(DeliveryMessage m) {
        Optional<Delivery> claimed = deliveries.claim(m.deliveryId(), props.delivery().lease());
        if (claimed.isEmpty()) {
            count(m.priority(), m.channel().name(), "duplicate_or_done");
            return;
        }
        Delivery d = claimed.get();

        if (d.attempt() > props.delivery().maxAttempts()) {
            deliveries.markFailed(d.deliveryId(), "max_attempts", DeliveryStatus.SENDING);
            count(d.priority(), d.channel().name(), "max_attempts");
            return;
        }

        // Zorluk #5 ve #7: mesaj kuyrukta beklerken izin geri çekilmiş ya da kampanya durdurulmuş olabilir.
        if (d.priority() == Priority.MARKETING) {
            if (campaigns.isStopped(d.campaignId())) {
                deliveries.markSkipped(d.deliveryId(), "campaign_stopped", DeliveryStatus.SENDING);
                count(d.priority(), d.channel().name(), "campaign_stopped");
                return;
            }
            if (!users.marketingAllowedNow(d.userId(), d.channel().name())) {
                deliveries.markSkipped(d.deliveryId(), "consent_revoked", DeliveryStatus.SENDING);
                count(d.priority(), d.channel().name(), "consent_revoked");
                return;
            }
        }

        List<ChannelProvider> chain = providers.chain(d.channel());
        if (!DeliveryPolicy.mayUseFallbackProvider(d.priority()) && !chain.isEmpty()) {
            chain = chain.subList(0, 1);
        }

        String lastReason = "no_provider";
        for (ChannelProvider provider : chain) {
            CircuitBreaker breaker = providers.breaker(provider.name());
            if (!breaker.tryAcquirePermission()) {
                lastReason = provider.name() + ":circuit_open";
                continue;
            }
            QuotaDecision q = quota.acquireWithin(provider.name(), d.priority(), DeliveryPolicy.maxQuotaWait(d.priority()));
            if (!q.allowed()) {
                breaker.releasePermission();
                lastReason = provider.name() + ":quota";
                continue;
            }

            deliveries.markSendingVia(d.deliveryId(), provider.name());
            long start = System.nanoTime();
            SendResult result;
            try {
                result = provider.send(new OutboundMessage(d.deliveryId(), d.channel(), d.recipient(), d.title(), d.body()));
            } catch (RuntimeException e) {
                result = new SendResult.Timeout(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            long tookNanos = System.nanoTime() - start;

            switch (result) {
                case SendResult.Accepted a -> {
                    breaker.onSuccess(tookNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
                    deliveries.markSent(d.deliveryId(), provider.name(), a.providerMsgId(), DeliveryStatus.SENDING);
                    count(d.priority(), d.channel().name(), "sent");
                    return;
                }
                case SendResult.PermanentFailure f -> {
                    breaker.onSuccess(tookNanos, java.util.concurrent.TimeUnit.NANOSECONDS);   // sağlayıcı sağlıklı, alıcı hatalı
                    deliveries.markFailed(d.deliveryId(), f.code(), DeliveryStatus.SENDING);
                    count(d.priority(), d.channel().name(), "failed");
                    return;
                }
                case SendResult.RateLimited r -> {
                    breaker.onSuccess(tookNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
                    aimd.onRateLimited(provider.name());
                    lastReason = provider.name() + ":429";
                }
                case SendResult.Unavailable u -> {
                    breaker.onError(tookNanos, java.util.concurrent.TimeUnit.NANOSECONDS, new ProviderException(u.detail()));
                    lastReason = provider.name() + ":" + u.detail();
                }
                case SendResult.Timeout t -> {
                    breaker.onError(tookNanos, java.util.concurrent.TimeUnit.NANOSECONDS, new ProviderException(t.detail()));
                    // Gitmiş olabilir: başka sağlayıcıyı DENEME. Önce bu sağlayıcıya sor.
                    deliveries.markUnknown(d.deliveryId(), provider.name(), t.detail());
                    count(d.priority(), d.channel().name(), "unknown");
                    deliveries.find(d.deliveryId())
                            .filter(x -> x.status() == DeliveryStatus.UNKNOWN)
                            .ifPresent(reconciler::reconcile);
                    return;
                }
            }
        }

        // Hiçbir sağlayıcı kabul etmedi: satırı bırak, Kafka kaydı yeniden denensin (partition bekler = backpressure).
        deliveries.releaseToPending(d.deliveryId(), lastReason);
        count(d.priority(), d.channel().name(), "retry_later");
        log.debug("Teslimat ertelendi {}: {}", d.deliveryId(), lastReason);
        throw new RetryLaterException(lastReason, DeliveryPolicy.retryDelayMs(d.priority()));
    }

    private void count(Priority priority, String channel, String result) {
        meters.counter("notify.delivery", "priority", priority.name(), "channel", channel, "result", result).increment();
    }

    static class ProviderException extends RuntimeException {
        ProviderException(String message) {
            super(message, null, false, false);
        }
    }
}
