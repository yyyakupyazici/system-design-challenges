package io.paramix.provision;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import io.paramix.provision.reconciliation.Reconciliation;
import io.paramix.provision.support.UuidV7;
import io.paramix.provision.withdrawal.FakeChainGateway;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Bütün entegrasyon testlerinin tabanı: gerçek PostgreSQL (Testcontainers), gerçek HTTP, gerçek eşzamanlılık.
 * Arka plan işleri (süpürücü, worker, relay) kapalı; testler onları elle, belirli anlarda çalıştırır.
 * Her testten sonra mutabakat çalışır: hiçbir senaryo defteri bozamaz.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "provision.expiry.enabled=false",
        "provision.withdrawals.worker-enabled=false",
        "provision.withdrawals.max-failures=3",
        "provision.outbox.relay-enabled=false",
        "provision.chain.fake-confirm-after-polls=0",
        "provision.idempotency.lock-wait-timeout=500ms",
        "spring.datasource.hikari.maximum-pool-size=30"
})
public abstract class IntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("provision").withUsername("provision").withPassword("provision");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    int port;

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected FakeChainGateway chain;

    @Autowired
    Reconciliation reconciliation;

    protected TestApi api;

    @BeforeEach
    void resetState() {
        jdbc.execute("TRUNCATE balances, provisions, ledger_entries, deposits, withdrawals, outbox, idempotency_keys, inbox CASCADE");
        chain.reset();
        api = new TestApi(port);
    }

    @AfterEach
    void booksMustBalance() {
        assertThat(reconciliation.run()).as("mutabakat farkı").isEmpty();
    }

    protected static String newKey() {
        return UuidV7.generate(Instant.now()).toString();
    }

    protected static String newRef() {
        return "order-" + UUID.randomUUID();
    }

    protected void fund(long customerId, String asset, String amount) {
        var r = api.deposit(customerId, asset, amount, "BANK", "eft-" + UUID.randomUUID());
        assertThat(r.status()).isEqualTo(201);
    }

    protected BigDecimal available(long customerId, String asset) {
        return jdbc.query("SELECT available FROM balances WHERE customer_id = ? AND asset = ?",
                (rs, i) -> rs.getBigDecimal(1), customerId, asset).stream().findFirst().orElse(BigDecimal.ZERO);
    }

    protected BigDecimal held(long customerId, String asset) {
        return jdbc.query("SELECT held FROM balances WHERE customer_id = ? AND asset = ?",
                (rs, i) -> rs.getBigDecimal(1), customerId, asset).stream().findFirst().orElse(BigDecimal.ZERO);
    }

    protected int count(String sql, Object... args) {
        return jdbc.queryForObject(sql, Integer.class, args);
    }
}
