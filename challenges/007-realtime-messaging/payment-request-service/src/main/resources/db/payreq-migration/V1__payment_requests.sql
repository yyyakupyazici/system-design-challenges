-- Para isteği. Kaynağı (origin) sohbet, ödeme linki ya da fatura olabilir: kurallar her kanal için aynı.
CREATE TABLE payment_requests (
    id                 bigserial PRIMARY KEY,
    requester_id       bigint NOT NULL,
    payer_id           bigint NOT NULL,
    amount_kurus       bigint NOT NULL CHECK (amount_kurus > 0),
    description        text   NOT NULL,
    origin_kind        text   NOT NULL CHECK (origin_kind IN ('CHAT', 'LINK', 'INVOICE', 'APP')),
    origin_ref         text,                                  -- CHAT ise sohbet id'si
    idempotency_key    text   NOT NULL,                       -- isteyenin cihazı üretir: "iste"ye iki kez basılsa da tek istek
    status             text   NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'PAID', 'DECLINED', 'EXPIRED', 'REVERSED')),
    version            int    NOT NULL DEFAULT 1,             -- her durum geçişinde +1; olaylar bununla sıralanır
    wallet_transfer_id text,
    reversal_reason    text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    expires_at         timestamptz NOT NULL,
    UNIQUE (requester_id, idempotency_key)
);
CREATE INDEX payment_requests_open ON payment_requests (status, updated_at) WHERE status IN ('PENDING', 'PROCESSING');
CREATE UNIQUE INDEX payment_requests_transfer ON payment_requests (wallet_transfer_id) WHERE wallet_transfer_id IS NOT NULL;
CREATE INDEX payment_requests_payer ON payment_requests (payer_id, status);

-- Her durum geçişi aynı transaction'da bir olay yazar; relay "payment.events" topic'ine taşır.
CREATE TABLE outbox (
    id         bigserial PRIMARY KEY,
    event_key  text NOT NULL,
    payload    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
