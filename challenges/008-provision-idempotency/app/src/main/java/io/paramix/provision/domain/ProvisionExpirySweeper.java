package io.paramix.provision.domain;

import io.paramix.provision.config.ProvisionProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Süresi dolan provizyonları serbest bırakır. Unutulan bir alım emri müşterinin parasını sonsuza dek kilitlememeli.
 * Her pod'da çalışabilir: SKIP LOCKED sayesinde süpürücüler aynı satırı almaz, capture/release ile yarışmaz.
 */
@Component
public class ProvisionExpirySweeper {

    private static final Logger log = LoggerFactory.getLogger(ProvisionExpirySweeper.class);

    private final ProvisionService provisions;
    private final ProvisionProperties.Expiry props;

    public ProvisionExpirySweeper(ProvisionService provisions, ProvisionProperties props) {
        this.provisions = provisions;
        this.props = props.expiry();
    }

    @Scheduled(fixedDelayString = "${provision.expiry.interval:1s}")
    public void tick() {
        if (!props.enabled()) {
            return;
        }
        try {
            sweep();
        } catch (RuntimeException e) {
            log.warn("Süre süpürücüsü turu başarısız: {}", e.toString());
        }
    }

    /** @return toplam serbest bırakılan */
    public int sweep() {
        int total = 0;
        int n;
        do {
            n = provisions.expireDue(props.batchSize());   // her parti ayrı transaction: kilitler kısa tutulur
            total += n;
        } while (n == props.batchSize());
        return total;
    }
}
