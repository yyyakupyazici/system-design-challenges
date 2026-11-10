-- Kullanıcı ve sohbetler
CREATE TABLE users (
    id              bigint PRIMARY KEY,
    name            text    NOT NULL,
    presence_hidden boolean NOT NULL DEFAULT false            -- "son görülme"yi gizle
);

CREATE TABLE conversations (
    id         bigint PRIMARY KEY,
    kind       text   NOT NULL CHECK (kind IN ('DIRECT', 'GROUP', 'BROADCAST')),
    title      text,
    last_seq   bigint NOT NULL DEFAULT 0,                      -- sohbet başına sıra sayacı (mesajla aynı transaction'da artar)
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE conversation_members (
    conversation_id bigint NOT NULL REFERENCES conversations (id),
    user_id         bigint NOT NULL REFERENCES users (id),
    role            text   NOT NULL DEFAULT 'MEMBER' CHECK (role IN ('MEMBER', 'ADMIN')),
    read_seq        bigint NOT NULL DEFAULT 0,                 -- bu kullanıcı nereye kadar OKUDU (bütün cihazları için)
    PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX conversation_members_user ON conversation_members (user_id);

-- Mesajlar. Kimlik = (sohbet, seq). Aynı istemci mesajı ikinci kez GİREMEZ (idempotency).
CREATE TABLE messages (
    conversation_id    bigint      NOT NULL,
    seq                bigint      NOT NULL,
    sender_id          bigint      NOT NULL,
    client_msg_id      uuid        NOT NULL,
    kind               text        NOT NULL CHECK (kind IN ('TEXT', 'PAYMENT_REQUEST', 'SYSTEM')),
    body               text        NOT NULL,
    payment_request_id bigint,
    server_time        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (conversation_id, seq),
    UNIQUE (conversation_id, client_msg_id)
);
CREATE INDEX messages_retention ON messages (server_time) WHERE payment_request_id IS NULL;

-- Cihaz başına "nereye kadar ALDIM" imleci: yeni bağlanan cihaz senkronu buradan başlar.
CREATE TABLE device_cursors (
    user_id         bigint NOT NULL,
    device_id       text   NOT NULL,
    conversation_id bigint NOT NULL,
    delivered_seq   bigint NOT NULL,
    PRIMARY KEY (user_id, device_id, conversation_id)
);

-- Para isteği: durum makinesi. PENDING → PROCESSING → PAID ; PROCESSING → PENDING (cüzdan reddetti) ; PENDING → EXPIRED / DECLINED
CREATE TABLE payment_requests (
    id                 bigserial PRIMARY KEY,
    conversation_id    bigint NOT NULL,
    requester_id       bigint NOT NULL,
    payer_id           bigint NOT NULL,
    amount_kurus       bigint NOT NULL CHECK (amount_kurus > 0),
    status             text   NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'PAID', 'DECLINED', 'EXPIRED')),
    wallet_transfer_id text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    expires_at         timestamptz NOT NULL
);
CREATE INDEX payment_requests_open ON payment_requests (status, updated_at) WHERE status IN ('PENDING', 'PROCESSING');

-- Transactional outbox: mesajla aynı transaction'da yazılır, relay Kafka'ya taşır.
CREATE TABLE outbox (
    id              bigserial PRIMARY KEY,
    conversation_id bigint NOT NULL,
    payload         text   NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
