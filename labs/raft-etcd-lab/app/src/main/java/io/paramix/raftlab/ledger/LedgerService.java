package io.paramix.raftlab.ledger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.paramix.raftlab.etcd.EtcdStore;
import io.paramix.raftlab.ha.HaAgent;
import io.paramix.raftlab.ha.HaAgent.Member;
import io.paramix.raftlab.pg.PgNode;

import org.springframework.stereotype.Service;

/**
 * Uygulamanın gözünden veritabanı. HAProxy'nin yaptığı işi burada kendimiz yapıyoruz:
 *
 * <pre>
 *  YAZMA ve KARAR VEREN OKUMA  → etcd'de lider kim? → onun PostgreSQL'i (primary)
 *  GÖSTERİLEN OKUMA            → istenen standby; "en az şu LSN'i görmüş ol" şartıyla (read-your-writes)
 * </pre>
 */
@Service
public class LedgerService {

    public record TransferResult(long transferId, String primary, String commitLsn) {
    }

    public record BalanceRead(String account, long balance, String servedBy, String replayLsn, long waitedMs, boolean fellBackToPrimary) {
    }

    private final EtcdStore etcd;
    private final HaAgent agent;

    public LedgerService(EtcdStore etcd, HaAgent agent) {
        this.etcd = etcd;
        this.agent = agent;
    }

    public Member primary() {
        String leader = etcd.leader().orElseThrow(() -> new IllegalStateException("lider yok: şu an yazılamaz")).node();
        Member m = agent.member(leader);
        if (m == null) {
            throw new IllegalStateException("lider " + leader + " ama adresi bilinmiyor");
        }
        return m;
    }

    public void seed(String account, long balance) throws SQLException {
        Member p = primary();
        try (Connection c = PgNode.connect(p.host(), p.port(), "ledger");
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO accounts (id, balance) VALUES (?, ?)
                     ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance""")) {
            ps.setString(1, account);
            ps.setLong(2, balance);
            ps.executeUpdate();
        }
    }

    /**
     * Karar veren okuma ("bakiye yeterli mi?") ile yazma AYNI transaction'da ve primary'de: koşullu UPDATE.
     * Commit, senkron standby "WAL diskimde" demeden dönmez (synchronous_commit = on).
     */
    public TransferResult transfer(String from, String to, long amount) throws SQLException {
        Member p = primary();
        return transferOn(p.host(), p.port(), p.node(), from, to, amount);
    }

    /** Belirli bir düğüme doğrudan yazmayı dener (split-brain deneyinde "eski primary'ye hâlâ yazan uygulama"). */
    public static TransferResult transferOn(String host, int port, String node, String from, String to, long amount) throws SQLException {
        try (Connection c = PgNode.connect(host, port, "ledger")) {
            c.setAutoCommit(false);
            try (PreparedStatement debit = c.prepareStatement("UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?");
                 PreparedStatement credit = c.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?");
                 PreparedStatement ins = c.prepareStatement("INSERT INTO transfers (from_acc, to_acc, amount) VALUES (?, ?, ?) RETURNING id")) {
                debit.setLong(1, amount);
                debit.setString(2, from);
                debit.setLong(3, amount);
                if (debit.executeUpdate() != 1) {
                    c.rollback();
                    throw new SQLException("bakiye yetersiz: " + from, "P0001");
                }
                credit.setLong(1, amount);
                credit.setString(2, to);
                credit.executeUpdate();
                ins.setString(1, from);
                ins.setString(2, to);
                ins.setLong(3, amount);
                long id;
                try (ResultSet rs = ins.executeQuery()) {
                    rs.next();
                    id = rs.getLong(1);
                }
                c.commit();
                c.setAutoCommit(true);
                try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_current_wal_lsn()")) {
                    rs.next();
                    return new TransferResult(id, node, rs.getString(1));
                }
            } catch (SQLException e) {
                try {
                    c.rollback();
                } catch (SQLException ignored) {
                    // bağlantı kopmuş olabilir
                }
                throw e;
            }
        }
    }

    /**
     * Gösterilen okuma. minLsn verilirse standby o LSN'i uygulayana kadar en fazla maxWaitMs bekler;
     * yetişemezse primary'den okur. minLsn yoksa standby'da ne varsa onu döner (bayat olabilir).
     */
    public BalanceRead balance(String account, String node, String minLsn, long maxWaitMs) throws SQLException {
        Member m = agent.member(node);
        if (m == null) {
            throw new IllegalStateException("düğüm bilinmiyor: " + node);
        }
        long start = System.currentTimeMillis();
        try (Connection c = PgNode.connect(m.host(), m.port(), "ledger")) {
            if (minLsn != null) {
                long need = PgNode.lsnToBytes(minLsn);
                while (PgNode.lsnToBytes(replayLsn(c)) < need) {
                    if (System.currentTimeMillis() - start > maxWaitMs) {
                        Member p = primary();
                        try (Connection pc = PgNode.connect(p.host(), p.port(), "ledger")) {
                            return new BalanceRead(account, read(pc, account), p.node(), "(primary)", System.currentTimeMillis() - start, true);
                        }
                    }
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SQLException(e);
                    }
                }
            }
            return new BalanceRead(account, read(c, account), node, replayLsn(c), System.currentTimeMillis() - start, false);
        }
    }

    private static String replayLsn(Connection c) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT CASE WHEN pg_is_in_recovery() THEN coalesce(pg_last_wal_replay_lsn(), '0/0') ELSE pg_current_wal_lsn() END")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static long read(Connection c, String account) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT balance FROM accounts WHERE id = ?")) {
            ps.setString(1, account);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        }
    }
}
