-- Kullanıcı iletişim bilgisi. Gerçek sistemde kullanıcı servisinden gelir; burada demo için tablo.
CREATE TABLE app_user (
    user_id     TEXT PRIMARY KEY,
    phone       TEXT,
    email       TEXT,
    locale      TEXT NOT NULL DEFAULT 'tr',
    time_zone   TEXT NOT NULL DEFAULT 'Europe/Istanbul'
);

-- Push token'ları kullanıcı değil KURULUM bazında tutulur: bir kullanıcının birden fazla cihazı olabilir.
CREATE TABLE device_token (
    installation_id TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES app_user (user_id),
    platform        TEXT NOT NULL CHECK (platform IN ('IOS', 'ANDROID')),
    token           TEXT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX device_token_user_idx ON device_token (user_id);

CREATE TABLE user_preference (
    user_id           TEXT PRIMARY KEY REFERENCES app_user (user_id),
    marketing_consent BOOLEAN NOT NULL DEFAULT FALSE,   -- yasal izin (İYS)
    sms_enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    email_enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    push_enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    quiet_start       TIME    NOT NULL DEFAULT '22:00',  -- kullanıcının kendi saat diliminde
    quiet_end         TIME    NOT NULL DEFAULT '08:00',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Şablonlar: dil + kanal bazında. Versiyon, değişiklik geçmişi ve geri alma için.
CREATE TABLE template (
    template_id TEXT    NOT NULL,
    locale      TEXT    NOT NULL,
    channel     TEXT    NOT NULL CHECK (channel IN ('SMS', 'EMAIL', 'PUSH')),
    version     INT     NOT NULL DEFAULT 1,
    title       TEXT,
    body        TEXT    NOT NULL,
    PRIMARY KEY (template_id, locale, channel)
);

CREATE TABLE campaign (
    campaign_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('RUNNING', 'STOPPED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ① API katmanı idempotency: aynı idempotency_key ikinci kez gelirse yeni bildirim oluşmaz.
CREATE TABLE notification (
    notification_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    type            TEXT NOT NULL,
    priority        TEXT NOT NULL CHECK (priority IN ('CRITICAL', 'TRANSACTIONAL', 'MARKETING')),
    user_id         TEXT NOT NULL,
    campaign_id     TEXT,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX notification_user_idx ON notification (user_id, created_at DESC);

-- Transactional outbox: bildirim kaydı ile "Kafka'ya yazılacak" kaydı AYNI transaction'da oluşur.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    msg_key      TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- ② / ③ Teslimat = bildirim × kanal. delivery_id deterministik: "<notification_id>:<CHANNEL>".
-- İçerik router'da render edilip burada saklanır; reaper yeniden gönderirken tekrar render etmez.
CREATE TABLE delivery (
    delivery_id     TEXT PRIMARY KEY,
    notification_id TEXT NOT NULL REFERENCES notification (notification_id),
    user_id         TEXT NOT NULL,
    channel         TEXT NOT NULL CHECK (channel IN ('SMS', 'EMAIL', 'PUSH')),
    priority        TEXT NOT NULL,
    campaign_id     TEXT,
    recipient       TEXT,
    title           TEXT,
    body            TEXT,
    status          TEXT NOT NULL CHECK (status IN (
                        'PENDING',        -- teslimat topic'inde, gönderilmeyi bekliyor
                        'SCHEDULED',      -- sessiz saat: not_before'da PENDING olacak
                        'FALLBACK_WAIT',  -- push'tan ack gelmezse not_before'da gönderilecek yedek kanal
                        'SENDING',        -- bir worker sahiplendi (locked_until'e kadar)
                        'SENT',           -- sağlayıcı kabul etti
                        'DELIVERED',      -- teslim raporu / uygulama ack'i geldi
                        'UNKNOWN',        -- gönderildi mi bilinmiyor (timeout, pod öldü)
                        'FAILED',
                        'SKIPPED')),      -- izin yok, kampanya durduruldu, ack geldi...
    provider        TEXT,
    provider_msg_id TEXT,
    attempt         INT  NOT NULL DEFAULT 0,
    not_before      TIMESTAMPTZ,
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX delivery_provider_msg_idx ON delivery (provider, provider_msg_id) WHERE provider_msg_id IS NOT NULL;
CREATE INDEX delivery_notification_idx ON delivery (notification_id);
-- Zamanlayıcıların taradığı durumlar için kısmi indeks
CREATE INDEX delivery_due_idx ON delivery (status, not_before) WHERE status IN ('SCHEDULED', 'FALLBACK_WAIT');
CREATE INDEX delivery_stuck_idx ON delivery (status, locked_until) WHERE status IN ('SENDING', 'UNKNOWN');
