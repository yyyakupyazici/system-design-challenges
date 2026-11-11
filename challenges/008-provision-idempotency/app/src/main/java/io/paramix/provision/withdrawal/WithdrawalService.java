package io.paramix.provision.withdrawal;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;

import io.paramix.provision.domain.AssetCatalog;
import io.paramix.provision.domain.DomainException;
import io.paramix.provision.domain.ErrorCode;
import io.paramix.provision.domain.Provision;
import io.paramix.provision.domain.ProvisionService;
import io.paramix.provision.outbox.Outbox;
import io.paramix.provision.support.UuidV7;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Çekim isteği. İstek yolu dış dünyaya HİÇ dokunmaz: tek transaction'da provizyon + PENDING çekim + olay yazar ve
 * 202 döner. Zincire gönderim {@link WithdrawalWorker}'ın işi.
 *
 * <p>Neden: zincir/banka çağrısı saniyeler sürebilir ve belirsiz bitebilir (timeout: gitti mi gitmedi mi?).
 * Bunu istek içinde yapmak; ya DB transaction'ını dış çağrı boyunca açık tutmak (bağlantı havuzu tükenir, satır
 * kilitleri uzar) ya da isteği birden çok transaction'a bölüp "IN_PROGRESS'te takılı kalan key" sorununu
 * API katmanına taşımak demek. Onun yerine çok adımlı süreç, kalıcı bir durum makinesine dönüşür ve kurtarma
 * mantığı tek bir yerde (worker) toplanır.
 */
@Service
public class WithdrawalService {

    public record RequestWithdrawal(long customerId, String asset, BigDecimal amount, String destination) {
    }

    private final ProvisionService provisions;
    private final WithdrawalRepository withdrawals;
    private final AssetCatalog assets;
    private final Outbox outbox;
    private final Clock clock;

    public WithdrawalService(ProvisionService provisions, WithdrawalRepository withdrawals, AssetCatalog assets,
                             Outbox outbox, Clock clock) {
        this.provisions = provisions;
        this.withdrawals = withdrawals;
        this.assets = assets;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public WithdrawalView request(RequestWithdrawal cmd) {
        UUID id = UuidV7.generate(clock);
        Provision hold = provisions.hold(new ProvisionService.HoldCommand(cmd.customerId(), cmd.asset(), cmd.amount(),
                Provision.OperationType.WITHDRAWAL, id.toString(), null));
        withdrawals.insert(id, cmd.customerId(), hold.asset(), hold.amount(), cmd.destination(), hold.id());
        WithdrawalView view = view(withdrawals.findById(id).orElseThrow());
        outbox.append("withdrawal", id, "withdrawal.requested", Map.of("customerId", cmd.customerId(), "withdrawal", view));
        return view;
    }

    public WithdrawalView get(long customerId, UUID id) {
        return withdrawals.findById(id)
                .filter(w -> w.customerId() == customerId)
                .map(this::view)
                .orElseThrow(() -> new DomainException(ErrorCode.WITHDRAWAL_NOT_FOUND, "Çekim yok: " + id));
    }

    WithdrawalView view(Withdrawal w) {
        return WithdrawalView.of(w, assets.require(w.asset()));
    }
}
