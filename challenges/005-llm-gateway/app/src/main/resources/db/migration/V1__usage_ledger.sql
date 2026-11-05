-- Kullanım defteri: maliyet raporları ve bütçe mutabakatının gerçeğin kaynağı.
-- Prod ölçeğinde: günlük partition (PARTITION BY RANGE (at)) ya da Kafka → ClickHouse.
CREATE TABLE usage_event (
    request_id         TEXT PRIMARY KEY,
    at                 TIMESTAMPTZ NOT NULL,
    team               TEXT NOT NULL,
    app                TEXT NOT NULL,
    user_id            TEXT NOT NULL,
    requested_model    TEXT NOT NULL,
    routed_model       TEXT,
    provider           TEXT,
    fallback_from      TEXT,
    cache_status       TEXT NOT NULL CHECK (cache_status IN ('MISS', 'HIT_EXACT', 'HIT_SEMANTIC')),
    prompt_cache       BOOLEAN NOT NULL DEFAULT FALSE,
    input_tokens       INT NOT NULL DEFAULT 0,
    cache_read_tokens  INT NOT NULL DEFAULT 0,
    cache_write_tokens INT NOT NULL DEFAULT 0,
    output_tokens      INT NOT NULL DEFAULT 0,
    cost_micro         BIGINT NOT NULL DEFAULT 0,      -- mikro-kredi (1 kredi = 1.000.000)
    latency_ms         BIGINT NOT NULL,
    ttft_ms            BIGINT,
    status             TEXT NOT NULL,                  -- ok, partial, failed
    pii_masked         INT NOT NULL DEFAULT 0,
    masked_prompt      TEXT,                           -- MASKELENMİŞ; 30 gün sonra NULL'lanır
    masked_response    TEXT
);
CREATE INDEX usage_event_team_at_idx ON usage_event (team, at DESC);
