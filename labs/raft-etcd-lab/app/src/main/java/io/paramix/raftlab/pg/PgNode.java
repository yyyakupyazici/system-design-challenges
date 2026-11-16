package io.paramix.raftlab.pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import io.paramix.raftlab.config.LabProperties;

import org.springframework.stereotype.Component;

/**
 * Ajanın kendi PostgreSQL'i üzerindeki işlemler. Hepsi gerçek PostgreSQL komutları:
 *
 * <pre>
 *  durum     pg_is_in_recovery(), pg_current_wal_lsn() / pg_last_wal_replay_lsn()
 *  promote   pg_promote()                                      standby → primary
 *  fence     ALTER SYSTEM SET default_transaction_read_only    primary'yi yazmaya kapat + açık bağlantıları kes
 *  senkron   ALTER SYSTEM SET synchronous_standby_names        "tamam" demeden kimi bekleyeceğim
 *  takip     ALTER SYSTEM SET primary_conninfo                  standby yeni primary'ye bağlansın (PG 13+, restart gerekmez)
 * </pre>
 *
 * Not: Gerçek Patroni fence için PostgreSQL'i durdurup standby olarak yeniden başlatır (demote). Lab, aynı etkiyi
 * daha görünür bir yolla yapıyor: yeni transaction'lar varsayılan olarak salt okunur, mevcut bağlantılar kesilir.
 */
@Component
public class PgNode {

    public record Status(boolean inRecovery, String lsn, long lsnBytes, boolean readOnly, String syncNames,
                         String primaryConninfo) {
    }

    private final LabProperties props;

    public PgNode(LabProperties props) {
        this.props = props;
    }

    public Connection connect(String database) throws SQLException {
        return connect(props.pgHost(), props.pgPort(), database);
    }

    public static Connection connect(String host, int port, String database) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", "postgres");
        p.setProperty("connectTimeout", "2");
        p.setProperty("socketTimeout", "10");
        p.setProperty("ApplicationName", "raft-lab");
        return DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database, p);
    }

    /** null = PostgreSQL'e ulaşılamıyor. */
    public Status statusOrNull() {
        try (Connection c = connect("postgres"); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("""
                SELECT pg_is_in_recovery(),
                       CASE WHEN pg_is_in_recovery() THEN coalesce(pg_last_wal_replay_lsn(), '0/0') ELSE pg_current_wal_lsn() END,
                       current_setting('default_transaction_read_only') = 'on',
                       current_setting('synchronous_standby_names'),
                       current_setting('primary_conninfo')""")) {
            rs.next();
            String lsn = rs.getString(2);
            return new Status(rs.getBoolean(1), lsn, lsnToBytes(lsn), rs.getBoolean(3), rs.getString(4), rs.getString(5));
        } catch (SQLException e) {
            return null;
        }
    }

    public void promote() throws SQLException {
        exec("postgres", "SELECT pg_promote(true, 30)");
    }

    /** Yazmaya kapat: yeni transaction'lar salt okunur, açık uygulama bağlantıları kesilir. */
    public void fence() throws SQLException {
        exec("postgres", "ALTER SYSTEM SET default_transaction_read_only = on");
        exec("postgres", "SELECT pg_reload_conf()");
        exec("postgres", """
                SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE backend_type = 'client backend' AND datname = 'ledger' AND pid <> pg_backend_pid()""");
    }

    public void unfence() throws SQLException {
        exec("postgres", "ALTER SYSTEM RESET default_transaction_read_only");
        exec("postgres", "SELECT pg_reload_conf()");
    }

    public void setSyncNames(String value) throws SQLException {
        exec("postgres", "ALTER SYSTEM SET synchronous_standby_names = '" + value.replace("'", "") + "'");
        exec("postgres", "SELECT pg_reload_conf()");
    }

    /** Bu standby şu an gerçekten senkron mu? (Primary'nin gözünden: pg_stat_replication.sync_state) */
    public boolean isSyncStandby(String name) throws SQLException {
        try (Connection c = connect("postgres");
             var ps = c.prepareStatement("SELECT 1 FROM pg_stat_replication WHERE application_name = ? AND sync_state IN ('sync', 'quorum')")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** Şu an bu primary'den WAL akıtan (gerçekten bağlı) standby'lar. etcd'deki üye kaydı ölü bir düğümü ttl boyunca gösterebilir; bu göstermez. */
    public java.util.Set<String> streamingStandbys() throws SQLException {
        java.util.Set<String> out = new java.util.TreeSet<>();
        try (Connection c = connect("postgres"); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT application_name FROM pg_stat_replication WHERE state = 'streaming'")) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }

    public void follow(String host, int port) throws SQLException {
        String conninfo = "host=" + host + " port=" + port + " user=postgres application_name=" + props.node();
        exec("postgres", "ALTER SYSTEM SET primary_conninfo = '" + conninfo + "'");
        exec("postgres", "SELECT pg_reload_conf()");
    }

    private void exec(String db, String sql) throws SQLException {
        try (Connection c = connect(db); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /** "0/3A000128" → bayt konumu (karşılaştırma için). */
    public static long lsnToBytes(String lsn) {
        if (lsn == null || !lsn.contains("/")) {
            return 0;
        }
        String[] p = lsn.split("/");
        return (Long.parseLong(p[0], 16) << 32) + Long.parseLong(p[1], 16);
    }
}
