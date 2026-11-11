package io.paramix.provision.domain;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import io.paramix.provision.domain.Ledger.Bucket;
import io.paramix.provision.domain.Ledger.EntryType;
import io.paramix.provision.outbox.Outbox;
import io.paramix.provision.support.UuidV7;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Yatırım: Idempotency-Key yerine DOĞAL anahtar. Banka EFT referansı ya da "zincir:tx_hash:log_index" dünyada
 * tektir; aynı yatırım banka webhook'undan, zincir dinleyicisinden ve gece mutabakatından üç kez gelse de
 * {@code UNIQUE (source, external_ref)} sayesinde bakiyeye bir kez yazılır.
 *
 * <p>Aynı referans farklı tutarla/müşteriyle gelirse sessizce yutulmaz: 409. Bu ya bir entegrasyon hatası ya da
 * saldırıdır, insan görmeli.
 */
@Service
public class DepositService {

    public enum Source { BANK, CHAIN }

    public record RecordDeposit(long customerId, String asset, BigDecimal amount, Source source, String externalRef) {
    }

    public record DepositView(UUID id, long customerId, String asset, String amount, Source source,
                              String externalRef, Instant createdAt) {
    }

    public record Result(DepositView deposit, boolean created) {
    }

    private final JdbcTemplate jdbc;
    private final AssetCatalog assets;
    private final BalanceRepository balances;
    private final Ledger ledger;
    private final Outbox outbox;
    private final Clock clock;

    public DepositService(JdbcTemplate jdbc, AssetCatalog assets, BalanceRepository balances, Ledger ledger,
                          Outbox outbox, Clock clock) {
        this.jdbc = jdbc;
        this.assets = assets;
        this.balances = balances;
        this.ledger = ledger;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public Result record(RecordDeposit cmd) {
        Asset asset = assets.require(cmd.asset());
        BigDecimal amount = asset.requireValidAmount(cmd.amount());
        UUID id = UuidV7.generate(clock);
        int inserted = jdbc.update("""
                INSERT INTO deposits (id, customer_id, asset, amount, source, external_ref)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT ON CONSTRAINT uq_deposit_external DO NOTHING
                """, id, cmd.customerId(), asset.code(), amount, cmd.source().name(), cmd.externalRef());
        if (inserted == 0) {
            DepositView existing = find(cmd.source(), cmd.externalRef());
            boolean same = existing.customerId() == cmd.customerId() && existing.asset().equals(asset.code())
                    && new BigDecimal(existing.amount()).compareTo(amount) == 0;
            if (!same) {
                throw new DomainException(ErrorCode.DEPOSIT_REF_CONFLICT,
                        "Referans " + cmd.externalRef() + " başka bir yatırıma ait",
                        Map.of("depositId", existing.id()));
            }
            return new Result(existing, false);
        }
        balances.credit(cmd.customerId(), asset.code(), amount);
        ledger.journal(cmd.customerId(), asset.code(), EntryType.DEPOSIT, id)
                .move(Bucket.EXTERNAL, Bucket.AVAILABLE, amount)
                .post();
        DepositView view = find(cmd.source(), cmd.externalRef());
        outbox.append("deposit", id, "deposit.credited", view);
        return new Result(view, true);
    }

    private DepositView find(Source source, String externalRef) {
        return jdbc.queryForObject("""
                        SELECT id, customer_id, asset, amount, source, external_ref, created_at
                        FROM deposits WHERE source = ? AND external_ref = ?
                        """,
                (rs, i) -> new DepositView(rs.getObject(1, UUID.class), rs.getLong(2), rs.getString(3),
                        assets.require(rs.getString(3)).format(rs.getBigDecimal(4)), Source.valueOf(rs.getString(5)),
                        rs.getString(6), rs.getTimestamp(7).toInstant()),
                source.name(), externalRef);
    }
}
