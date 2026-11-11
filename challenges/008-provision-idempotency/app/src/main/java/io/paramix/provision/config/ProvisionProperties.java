package io.paramix.provision.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("provision")
public record ProvisionProperties(Idempotency idempotency, Holds holds, Expiry expiry, Withdrawals withdrawals,
                                  Outbox outbox, Chain chain) {

    /**
     * @param acceptWindow   key'in doğum zamanı bundan eskiyse istek REDDEDİLİR (yeni key ile gelmeli).
     * @param maxClockSkew   istemci saati ileri olabilir; bu kadar "gelecekten" gelen key kabul.
     * @param retention      kayıtlar en az bu kadar tutulur (günlük partition'lar bundan sonra DROP edilir).
     * @param lockWaitTimeout aynı key ile eşzamanlı ikinci istek, ilki bitene kadar en fazla bu kadar bekler; sonra 409.
     */
    public record Idempotency(Duration acceptWindow, Duration maxClockSkew, Duration retention,
                              Duration lockWaitTimeout, int partitionsAheadDays) {

        public Idempotency {
            // Altın kural: kabul edilen her key'in kaydı hâlâ duruyor olmalı. Kaydı silinmiş olabilecek bir key
            // "ilk kez görüyorum" diye işlenirse aynı işlem iki kez yapılır. Bu yüzden saklama süresi kabul
            // penceresinden kısa olamaz; yanlış ayar uygulamayı hiç başlatmaz.
            if (retention.compareTo(acceptWindow.plus(maxClockSkew).plusHours(1)) < 0) {
                throw new IllegalStateException("provision.idempotency.retention (" + retention
                        + ") accept-window + max-clock-skew + 1h'den kısa olamaz (" + acceptWindow + " + " + maxClockSkew + ")");
            }
            if (partitionsAheadDays < 1) {
                throw new IllegalStateException("provision.idempotency.partitions-ahead-days en az 1 olmalı");
            }
        }
    }

    public record Holds(Duration defaultTtl, Duration maxTtl) {
    }

    public record Expiry(boolean enabled, int batchSize) {
    }

    /**
     * @param lease          worker bir çekimi bu kadar süre için kiralar; süre dolarsa başka worker devralabilir.
     * @param confirmationPollDelay zincirde bekleyen işlem bu aralıkla tekrar sorulur.
     * @param maxFailures    bu kadar ardışık hatadan sonra çekim MANUAL_REVIEW'a düşer (otomatik iade YOK).
     */
    public record Withdrawals(boolean workerEnabled, Duration lease, Duration confirmationPollDelay,
                              Duration retryBackoff, int maxFailures) {
    }

    public record Outbox(boolean relayEnabled, int batchSize) {
    }

    /** Demo zinciri: kaç durum sorgusundan sonra işlemi kendiliğinden onaylasın (0 = asla, testler elle onaylar). */
    public record Chain(int fakeConfirmAfterPolls) {
    }
}
