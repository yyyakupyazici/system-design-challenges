package io.paylink.gateway.policy;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * PostgreSQL erişimi. Bloklayan JDBC kullanır; bu yüzden ASLA istek yolunda (event loop'ta)
 * çağrılmaz: başlangıçta, zamanlanmış yenilemede ve admin API'de boundedElastic üzerinde çağrılır.
 */
@Repository
public class PolicyRepository {

    private final JdbcClient jdbc;

    public PolicyRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<RateLimitPolicy> findAllPolicies() {
        return jdbc.sql("""
                        SELECT id, scope_type, scope_id, endpoint, rate_per_sec, burst, daily_quota, version
                        FROM rate_limit_policy
                        """)
                .query(PolicyRepository::mapPolicy)
                .list();
    }

    public List<ApiKeyRecord> findAllApiKeys() {
        return jdbc.sql("SELECT key_hash, tenant_id, plan, active FROM api_key")
                .query((rs, i) -> new ApiKeyRecord(
                        rs.getString("key_hash"), rs.getString("tenant_id"),
                        rs.getString("plan"), rs.getBoolean("active")))
                .list();
    }

    /** Kurallar veya key'ler değiştiyse bu değer artar (ortak sequence). */
    public long maxVersion() {
        return jdbc.sql("""
                        SELECT GREATEST(COALESCE((SELECT max(version) FROM rate_limit_policy), 0),
                                        COALESCE((SELECT max(version) FROM api_key), 0))
                        """)
                .query(Long.class)
                .single();
    }

    /** Kuralı ekler/günceller ve aynı ifadede audit kaydı yazar. */
    @Transactional
    public RateLimitPolicy upsert(ScopeType scopeType, String scopeId, EndpointPattern endpoint,
                                  int ratePerSec, int burst, Long dailyQuota, String updatedBy) {
        return jdbc.sql("""
                        WITH up AS (
                          INSERT INTO rate_limit_policy
                            (scope_type, scope_id, endpoint, rate_per_sec, burst, daily_quota, version, updated_by)
                          VALUES (:scopeType, :scopeId, :endpoint, :rate, :burst, :quota,
                                  nextval('policy_version_seq'), :updatedBy)
                          ON CONFLICT (scope_type, scope_id, endpoint) DO UPDATE
                            SET rate_per_sec = EXCLUDED.rate_per_sec,
                                burst        = EXCLUDED.burst,
                                daily_quota  = EXCLUDED.daily_quota,
                                version      = EXCLUDED.version,
                                updated_by   = EXCLUDED.updated_by,
                                updated_at   = now()
                          RETURNING *
                        ), audit AS (
                          INSERT INTO rate_limit_policy_audit
                            (policy_id, scope_type, scope_id, endpoint, rate_per_sec, burst, daily_quota, version, changed_by)
                          SELECT id, scope_type, scope_id, endpoint, rate_per_sec, burst, daily_quota, version, updated_by
                          FROM up
                        )
                        SELECT id, scope_type, scope_id, endpoint, rate_per_sec, burst, daily_quota, version FROM up
                        """)
                .param("scopeType", scopeType.name())
                .param("scopeId", scopeId)
                .param("endpoint", endpoint.raw())
                .param("rate", ratePerSec)
                .param("burst", burst)
                .param("quota", new SqlParameterValue(Types.BIGINT, dailyQuota))
                .param("updatedBy", updatedBy)
                .query(PolicyRepository::mapPolicy)
                .single();
    }

    public void insertApiKey(String keyHash, String tenantId, String plan) {
        jdbc.sql("INSERT INTO api_key (key_hash, tenant_id, plan) VALUES (:hash, :tenant, :plan)")
                .param("hash", keyHash)
                .param("tenant", tenantId)
                .param("plan", plan)
                .update();
    }

    private static RateLimitPolicy mapPolicy(ResultSet rs, int rowNum) throws SQLException {
        long quota = rs.getLong("daily_quota");
        Long dailyQuota = rs.wasNull() ? null : quota;
        return new RateLimitPolicy(
                rs.getLong("id"),
                ScopeType.valueOf(rs.getString("scope_type")),
                rs.getString("scope_id"),
                EndpointPattern.parse(rs.getString("endpoint")),
                rs.getInt("rate_per_sec"),
                rs.getInt("burst"),
                dailyQuota,
                rs.getLong("version"));
    }
}
