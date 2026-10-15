-- Kuralların kalıcı kaynağı (source of truth). Gateway pod'ları bunu belleğe kopyalar.

-- Her değişiklik global olarak artan bir versiyon alır; pod'lar max(version) ile değişikliği fark eder.
CREATE SEQUENCE policy_version_seq;

CREATE TABLE rate_limit_policy (
    id            BIGSERIAL PRIMARY KEY,
    scope_type    TEXT        NOT NULL CHECK (scope_type IN ('PLAN', 'TENANT')),
    scope_id      TEXT        NOT NULL,              -- 'PRO' | 'tenant_42'
    endpoint      TEXT        NOT NULL DEFAULT '*',  -- '*' (genel kova) veya 'POST /payments', 'GET /payments/**'
    rate_per_sec  INT         NOT NULL CHECK (rate_per_sec > 0),
    burst         INT         NOT NULL CHECK (burst >= rate_per_sec),
    daily_quota   BIGINT      CHECK (daily_quota IS NULL OR daily_quota >= 0), -- NULL = devral / tanımsız
    version       BIGINT      NOT NULL DEFAULT nextval('policy_version_seq'),
    updated_by    TEXT        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (scope_type, scope_id, endpoint)
);

-- "Kim, ne zaman, hangi limiti değiştirdi?" sorusunun cevabı.
CREATE TABLE rate_limit_policy_audit (
    id            BIGSERIAL PRIMARY KEY,
    policy_id     BIGINT      NOT NULL,
    scope_type    TEXT        NOT NULL,
    scope_id      TEXT        NOT NULL,
    endpoint      TEXT        NOT NULL,
    rate_per_sec  INT         NOT NULL,
    burst         INT         NOT NULL,
    daily_quota   BIGINT,
    version       BIGINT      NOT NULL,
    changed_by    TEXT        NOT NULL,
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- B2B müşterilere verdiğimiz API key'ler. Düz metin asla saklanmaz, SHA-256 hash'i saklanır.
CREATE TABLE api_key (
    key_hash    TEXT        PRIMARY KEY,
    tenant_id   TEXT        NOT NULL,
    plan        TEXT        NOT NULL CHECK (plan IN ('FREE', 'PRO', 'ENTERPRISE')),
    active      BOOLEAN     NOT NULL DEFAULT TRUE,
    version     BIGINT      NOT NULL DEFAULT nextval('policy_version_seq'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_key_tenant ON api_key (tenant_id);
