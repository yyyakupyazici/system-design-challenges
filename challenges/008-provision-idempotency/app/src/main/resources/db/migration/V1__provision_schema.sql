-- =====================================================================================================================
-- Provizyon servisi şeması.
--
-- İlke: "doğruluk kodda değil, veritabanı kısıtlarında". Uygulamada bir hata olsa bile
--   * bakiye eksiye düşemez                       (CHECK available >= 0, held >= 0)
--   * aynı iş için iki provizyon açılamaz          (UNIQUE customer_id, operation_type, operation_ref)
--   * aynı yatırım iki kez yazılamaz               (UNIQUE source, external_ref)
--   * aynı Idempotency-Key iki kez işlenemez       (PRIMARY KEY customer_id, idem_key, key_day)
--   * ledger geriye dönük değiştirilemez           (UPDATE/DELETE trigger ile yasak)
-- Bütün tutarlar NUMERIC: kayan nokta (double) parada kullanılmaz.
-- =====================================================================================================================

-- ---------------------------------------------------------------------------------------------------------------------
-- Varlıklar. scale = o varlığın en küçük birimi (TRY kuruş: 2, BTC satoshi: 8, ETH wei: 18).
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE assets (
    code  TEXT     PRIMARY KEY CHECK (code ~ '^[A-Z0-9]{2,10}$'),
    kind  TEXT     NOT NULL CHECK (kind IN ('FIAT', 'CRYPTO')),
    scale SMALLINT NOT NULL CHECK (scale BETWEEN 0 AND 18)
);

INSERT INTO assets (code, kind, scale) VALUES
    ('TRY',  'FIAT',   2),
    ('USD',  'FIAT',   2),
    ('EUR',  'FIAT',   2),
    ('BTC',  'CRYPTO', 8),
    ('ETH',  'CRYPTO', 18),
    ('USDT', 'CRYPTO', 6);

-- ---------------------------------------------------------------------------------------------------------------------
-- Bakiye: müşteri × varlık başına tek satır, iki kova.
--   available  kullanılabilir
--   held       provizyonda (bloke). Toplam varlık = available + held.
-- Bakiye bir "önbellek"tir: gerçeğin kaynağı ledger_entries. Mutabakat ikisinin eşitliğini sürekli doğrular.
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE balances (
    customer_id BIGINT         NOT NULL,
    asset       TEXT           NOT NULL REFERENCES assets (code),
    available   NUMERIC(38, 18) NOT NULL DEFAULT 0 CHECK (available >= 0),
    held        NUMERIC(38, 18) NOT NULL DEFAULT 0 CHECK (held >= 0),
    version     BIGINT         NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, asset)
);

-- ---------------------------------------------------------------------------------------------------------------------
-- Provizyon (hold). Durum makinesi:
--
--      HELD ──capture──▶ CAPTURED      (kısmi capture: kalan otomatik serbest kalır)
--        │ ──release──▶ RELEASED
--        └ ──süre────▶ EXPIRED         (sadece expires_at dolu olanlar; çekim provizyonlarının süresi YOKTUR)
--
-- Her geçiş "WHERE status = 'HELD'" koşullu UPDATE'tir: iki istek aynı anda capture etmeye çalışsa da biri kazanır.
-- (customer_id, operation_type, operation_ref) UNIQUE = iş seviyesinde idempotency: aynı emir için ikinci provizyon,
-- farklı bir Idempotency-Key ile gelse bile açılamaz.
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE provisions (
    id              UUID           PRIMARY KEY,
    customer_id     BIGINT         NOT NULL,
    asset           TEXT           NOT NULL REFERENCES assets (code),
    amount          NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    captured_amount NUMERIC(38, 18),
    operation_type  TEXT           NOT NULL CHECK (operation_type IN ('TRADE', 'WITHDRAWAL', 'TRANSFER')),
    operation_ref   TEXT           NOT NULL CHECK (length(operation_ref) BETWEEN 1 AND 128),
    status          TEXT           NOT NULL CHECK (status IN ('HELD', 'CAPTURED', 'RELEASED', 'EXPIRED')),
    expires_at      TIMESTAMPTZ,
    version         INT            NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_provision_operation UNIQUE (customer_id, operation_type, operation_ref),
    CONSTRAINT ck_captured_amount CHECK (
        (status = 'CAPTURED' AND captured_amount IS NOT NULL AND captured_amount > 0 AND captured_amount <= amount)
        OR (status <> 'CAPTURED' AND captured_amount IS NULL)),
    CONSTRAINT ck_withdrawal_never_expires CHECK (operation_type <> 'WITHDRAWAL' OR expires_at IS NULL)
);

CREATE INDEX ix_provisions_customer ON provisions (customer_id, created_at DESC);
-- Süre süpürücüsünün tek sorgusu: sadece HELD + süreli satırları içeren küçük, kısmi indeks.
CREATE INDEX ix_provisions_expiry ON provisions (expires_at) WHERE status = 'HELD' AND expires_at IS NOT NULL;

-- ---------------------------------------------------------------------------------------------------------------------
-- Ledger: append-only, çift kayıt. Her journal_id içindeki satırların toplamı (varlık başına) SIFIR.
--   AVAILABLE / HELD  müşterinin bakiye kovaları (balances ile eşleşir)
--   EXTERNAL          dış dünya karşı kaydı (yatırım buradan gelir)
--   SETTLED           capture ile hesaptan çıkan para (işlem/çekim tarafına geçti)
-- Sistem hesapları için balances satırı TUTULMAZ: tek bir "sistem hesabı" satırı, bütün işlemlerin aynı satırı
-- kilitlediği sıcak bir nokta (hot row) olurdu.
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    journal_id  UUID           NOT NULL,
    customer_id BIGINT         NOT NULL,
    asset       TEXT           NOT NULL REFERENCES assets (code),
    bucket      TEXT           NOT NULL CHECK (bucket IN ('AVAILABLE', 'HELD', 'EXTERNAL', 'SETTLED')),
    delta       NUMERIC(38, 18) NOT NULL CHECK (delta <> 0),
    entry_type  TEXT           NOT NULL CHECK (entry_type IN ('DEPOSIT', 'HOLD', 'CAPTURE', 'RELEASE', 'EXPIRE')),
    ref_id      UUID           NOT NULL,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX ix_ledger_customer ON ledger_entries (customer_id, asset, id);
CREATE INDEX ix_ledger_ref ON ledger_entries (ref_id);
CREATE INDEX ix_ledger_journal ON ledger_entries (journal_id);

CREATE FUNCTION ledger_forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries append-only: % yasak (düzeltme ters kayıtla yapılır)', TG_OP
        USING ERRCODE = 'restrict_violation';
END $$;

CREATE TRIGGER trg_ledger_append_only
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_mutation();

-- ---------------------------------------------------------------------------------------------------------------------
-- Yatırım (deposit). Idempotency doğal anahtarla: banka referansı ya da "zincir:tx_hash:log_index".
-- Aynı olay webhook'tan, zincir dinleyicisinden ve mutabakattan üç kez gelse de bir kez yazılır.
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE deposits (
    id           UUID           PRIMARY KEY,
    customer_id  BIGINT         NOT NULL,
    asset        TEXT           NOT NULL REFERENCES assets (code),
    amount       NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    source       TEXT           NOT NULL CHECK (source IN ('BANK', 'CHAIN')),
    external_ref TEXT           NOT NULL CHECK (length(external_ref) BETWEEN 1 AND 200),
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_deposit_external UNIQUE (source, external_ref)
);

-- ---------------------------------------------------------------------------------------------------------------------
-- Çekim (withdrawal). İstek yolu tek transaction'dır (provizyon + PENDING kaydı); zincire gönderim worker'da,
-- adım adım ve her adımın sonucu kalıcı yazılarak yapılır:
--
--   PENDING ──imzala, tx_hash'i YAZ──▶ SIGNED ──yayınla──▶ BROADCAST ──onay──▶ CONFIRMED (provizyon capture)
--                                                                 └──red───▶ FAILED    (provizyon release)
--   tekrarlayan hata ──▶ MANUAL_REVIEW  (para zincirde olabilir: otomatik iade YAPILMAZ)
--
-- claim_token + lease_until: worker'ın kiraladığı kilit. Süre dolarsa başka worker devralır; eski worker'ın
-- yazmaları "WHERE claim_token = :benimki" koşuluna takılır (fencing).
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE withdrawals (
    id              UUID           PRIMARY KEY,
    customer_id     BIGINT         NOT NULL,
    asset           TEXT           NOT NULL REFERENCES assets (code),
    amount          NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    destination     TEXT           NOT NULL CHECK (length(destination) BETWEEN 1 AND 200),
    provision_id    UUID           NOT NULL UNIQUE REFERENCES provisions (id),
    status          TEXT           NOT NULL CHECK (status IN ('PENDING', 'SIGNED', 'BROADCAST', 'CONFIRMED', 'FAILED', 'MANUAL_REVIEW')),
    tx_hash         TEXT           UNIQUE,
    signed_payload  TEXT,
    claim_token     UUID,
    lease_until     TIMESTAMPTZ,
    failures        INT            NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    -- İmzalanmış işlem yayınlanmadan ÖNCE kalıcı olmalı: yoksa çökme sonrası "gönderdim mi?" sorusu cevapsız kalır.
    CONSTRAINT ck_signed_before_broadcast CHECK (
        status NOT IN ('SIGNED', 'BROADCAST', 'CONFIRMED') OR (tx_hash IS NOT NULL AND signed_payload IS NOT NULL)),
    CONSTRAINT ck_claim_pair CHECK ((claim_token IS NULL) = (lease_until IS NULL))
);

CREATE INDEX ix_withdrawals_due ON withdrawals (next_attempt_at) WHERE status IN ('PENDING', 'SIGNED', 'BROADCAST');
CREATE INDEX ix_withdrawals_customer ON withdrawals (customer_id, created_at DESC);

-- ---------------------------------------------------------------------------------------------------------------------
-- Outbox: iş verisiyle AYNI transaction'da yazılan olaylar. Relay en az bir kez yayınlar; event_id tüketicinin
-- inbox'ında tekilleştirme anahtarıdır.
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE outbox (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   UUID        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------------------------------------------------
-- API idempotency kayıtları. Günlük RANGE partition.
--
-- Sorun: PostgreSQL'de partition'lı tablodaki PRIMARY KEY/UNIQUE, partition anahtarını İÇERMEK ZORUNDA.
-- created_at'e göre bölseydik aynı key gün sınırında iki farklı partition'a yazılabilirdi → tekilleştirme kırılır.
-- Çözüm: key_day, istemcinin gönderdiği UUIDv7'nin İÇİNDEKİ zamandan türetilir. Aynı key → her zaman aynı gün →
-- her zaman aynı partition. Böylece PK (customer_id, idem_key, key_day) gerçekten "key başına tek satır" demektir
-- ve eski günler DELETE yerine DROP TABLE ile, şişkinlik (bloat) ve vacuum yükü olmadan atılır.
--
-- response_body TEXT (JSONB değil): JSONB anahtar sırasını ve boşlukları normalize eder. Tekrar oynatılan cevap,
-- ilk cevapla bayt bayt aynı olmalı (istemci imza/hash doğruluyor olabilir).
--
-- response_* kolonları satır ilk eklendiğinde NULL'dır ve AYNI transaction içinde doldurulur. Başka bir
-- transaction bu satırı ancak commit'ten sonra görür, o anda da cevap doludur.
-- ---------------------------------------------------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    key_day         DATE        NOT NULL,
    customer_id     BIGINT      NOT NULL,
    idem_key        UUID        NOT NULL,
    operation       TEXT        NOT NULL,
    request_hash    BYTEA       NOT NULL CHECK (octet_length(request_hash) = 32),
    response_status SMALLINT,
    response_body   TEXT,
    resource_id     UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    PRIMARY KEY (customer_id, idem_key, key_day)
) PARTITION BY RANGE (key_day);

-- Partition yönetimi veritabanında: birden çok pod aynı anda çağırsa da advisory lock ile tek seferde yapılır.
CREATE FUNCTION idempotency_ensure_partitions(p_from DATE, p_to DATE) RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
    d       DATE := p_from;
    created INT  := 0;
    part    TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('idempotency_keys_partitions'));
    WHILE d <= p_to LOOP
        part := format('idempotency_keys_%s', to_char(d, 'YYYYMMDD'));
        IF to_regclass(part) IS NULL THEN
            EXECUTE format('CREATE TABLE %I PARTITION OF idempotency_keys FOR VALUES FROM (%L) TO (%L)', part, d, d + 1);
            created := created + 1;
        END IF;
        d := d + 1;
    END LOOP;
    RETURN created;
END $$;

CREATE FUNCTION idempotency_drop_partitions_before(p_day DATE) RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
    part    RECORD;
    dropped INT := 0;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('idempotency_keys_partitions'));
    FOR part IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'idempotency_keys'::regclass
          AND c.relname ~ '^idempotency_keys_\d{8}$'
          AND to_date(right(c.relname, 8), 'YYYYMMDD') < p_day
    LOOP
        EXECUTE format('DROP TABLE %I', part.relname);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END $$;

-- İlk açılış için birkaç gün; sonrası uygulamadaki PartitionMaintenance işi.
SELECT idempotency_ensure_partitions((now() AT TIME ZONE 'UTC')::date - 2, (now() AT TIME ZONE 'UTC')::date + 3);
