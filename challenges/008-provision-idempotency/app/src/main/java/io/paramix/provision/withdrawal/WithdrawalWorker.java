package io.paramix.provision.withdrawal;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.paramix.provision.config.ProvisionProperties;
import io.paramix.provision.domain.ProvisionService;
import io.paramix.provision.outbox.Outbox;
import io.paramix.provision.withdrawal.ChainGateway.SignedTransaction;
import io.paramix.provision.withdrawal.ChainGateway.TransferInstruction;
import io.paramix.provision.withdrawal.ChainGateway.TxStatus;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Çekimleri zincire taşıyan worker. Her adımın sonucu kalıcı yazılır; süreç herhangi bir noktada ölse de
 * kirası dolunca başka bir worker kaldığı yerden, ÇİFT GÖNDERİM YAPMADAN devam eder.
 *
 * <pre>
 *   PENDING ──① imzala, hash'i YAZ──▶ SIGNED ──② zincire sor, yoksa yayınla──▶ BROADCAST ──③ onay?──┐
 *                                                                                                ├ CONFIRMED → capture
 *                                                                                                ├ FAILED    → release
 *                                                                                                └ bekliyor  → sonra tekrar sor
 * </pre>
 *
 * <b>Çökme noktaları ve neden güvenli olduğu:</b>
 * <ul>
 *   <li>① imzadan sonra, yazmadan önce: hiçbir şey yayınlanmadı. Devralan yeniden imzalar.</li>
 *   <li>② yayından sonra, BROADCAST yazmadan önce: durum SIGNED kalır. Devralan yayından ÖNCE hash ile zincire
 *       sorar; işlem oradaysa tekrar yayınlamaz. Sorgu yanılsa bile aynı imzalı baytları yollar, zincir ikinciyi
 *       kabul etmez. İki kat koruma.</li>
 *   <li>③ onaydan sonra, capture'dan önce: capture ve CONFIRMED tek transaction. Devralan tekrar sorar, CONFIRMED
 *       görür, capture eder.</li>
 * </ul>
 *
 * <b>Fencing:</b> kirası devralınmış worker'ın her yazması {@code claim_token} koşuluna takılır
 * ({@link LostLeaseException}). DB'yi fencing, zinciri aynı imzalı işlem korur; ikisi birlikte gerekli.
 */
@Component
public class WithdrawalWorker {

    private static final Logger log = LoggerFactory.getLogger(WithdrawalWorker.class);

    /** Kiralanmış iş: token, bu kiranın kimliği (fencing token). */
    public record Claim(UUID token, Withdrawal withdrawal) {
    }

    private final WithdrawalRepository repo;
    private final ProvisionService provisions;
    private final ChainGateway chain;
    private final Outbox outbox;
    private final WithdrawalService service;
    private final TransactionTemplate tx;
    private final ProvisionProperties.Withdrawals props;
    private final MeterRegistry meters;

    public WithdrawalWorker(WithdrawalRepository repo, ProvisionService provisions, ChainGateway chain, Outbox outbox,
                            WithdrawalService service, TransactionTemplate tx, ProvisionProperties props,
                            MeterRegistry meters) {
        this.repo = repo;
        this.provisions = provisions;
        this.chain = chain;
        this.outbox = outbox;
        this.service = service;
        this.tx = tx;
        this.props = props.withdrawals();
        this.meters = meters;
    }

    @Scheduled(fixedDelayString = "${provision.withdrawals.poll-interval:500ms}")
    public void tick() {
        if (!props.workerEnabled()) {
            return;
        }
        for (int i = 0; i < 100 && runOnce(); i++) {
            // sırada iş oldukça devam
        }
    }

    public boolean runOnce() {
        Optional<Claim> claim = claimNext();
        claim.ifPresent(this::process);
        return claim.isPresent();
    }

    public Optional<Claim> claimNext() {
        UUID token = UUID.randomUUID();
        return repo.claimNext(token, props.lease()).map(w -> new Claim(token, w));
    }

    public void process(Claim claim) {
        Withdrawal w = claim.withdrawal();
        try {
            if (w.status() == Withdrawal.Status.PENDING) {
                w = sign(claim, w);
            }
            if (w.status() == Withdrawal.Status.SIGNED) {
                w = broadcast(claim, w);
            }
            if (w.status() == Withdrawal.Status.BROADCAST) {
                settle(claim, w);
            }
        } catch (LostLeaseException e) {
            meters.counter("withdrawals.lost_lease").increment();
            log.warn("{}: hiçbir şey yazılmadan çekiliniyor", e.getMessage());
        } catch (RuntimeException e) {
            meters.counter("withdrawals.step_failures").increment();
            log.warn("Çekim {} adımı başarısız: {}", w.id(), e.toString());
            repo.recordFailure(w.id(), claim.token(), e.toString(), props.retryBackoff(), props.maxFailures());
        }
    }

    private Withdrawal sign(Claim claim, Withdrawal w) {
        SignedTransaction signed = chain.sign(new TransferInstruction(w.id(), w.asset(), w.amount(), w.destination()));
        fenced(repo.markSigned(w.id(), claim.token(), signed.txHash(), signed.payload()), w.id());
        return reload(w.id());
    }

    private Withdrawal broadcast(Claim claim, Withdrawal w) {
        if (chain.status(w.txHash()) == TxStatus.NOT_FOUND) {
            chain.broadcast(new SignedTransaction(w.txHash(), w.signedPayload()));
        } else {
            // Kurtarma yolu: önceki sahip yayınlamış ama sonucu yazamadan ölmüş.
            meters.counter("withdrawals.recovered_broadcast").increment();
            log.info("Çekim {} zaten zincirde ({}), tekrar yayınlanmıyor", w.id(), w.txHash());
        }
        fenced(repo.markBroadcast(w.id(), claim.token()), w.id());
        return reload(w.id());
    }

    private void settle(Claim claim, Withdrawal w) {
        TxStatus status = chain.status(w.txHash());
        switch (status) {
            case CONFIRMED -> finish(claim, w, Withdrawal.Status.CONFIRMED);
            case FAILED -> finish(claim, w, Withdrawal.Status.FAILED);
            case NOT_FOUND -> {
                // Mempool'dan düşmüş olabilir: AYNI baytları tekrar yolla (yeniden imzalama yok).
                chain.broadcast(new SignedTransaction(w.txHash(), w.signedPayload()));
                fenced(repo.reschedule(w.id(), claim.token(), props.confirmationPollDelay()), w.id());
            }
            case PENDING -> fenced(repo.reschedule(w.id(), claim.token(), props.confirmationPollDelay()), w.id());
        }
    }

    /** Çekimin son durumu ve provizyonun kapanışı TEK transaction: biri olup diğeri olamaz. */
    private void finish(Claim claim, Withdrawal w, Withdrawal.Status result) {
        tx.executeWithoutResult(s -> {
            fenced(repo.markFinal(w.id(), claim.token(), result), w.id());   // önce fencing: satır kilidi de alınır
            if (result == Withdrawal.Status.CONFIRMED) {
                provisions.capture(null, w.provisionId(), null);
            } else {
                provisions.release(null, w.provisionId());
            }
            Withdrawal done = reload(w.id());
            outbox.append("withdrawal", w.id(), "withdrawal." + result.name().toLowerCase(),
                    Map.of("customerId", w.customerId(), "withdrawal", service.view(done)));
        });
        meters.counter("withdrawals.finished", "result", result.name()).increment();
    }

    private Withdrawal reload(UUID id) {
        return repo.findById(id).orElseThrow();
    }

    private static void fenced(boolean updated, UUID withdrawalId) {
        if (!updated) {
            throw new LostLeaseException(withdrawalId);
        }
    }
}
