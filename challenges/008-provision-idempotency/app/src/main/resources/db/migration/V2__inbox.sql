-- =====================================================================================================================
-- Inbox: tüketici tarafında tekilleştirme. Outbox'ın aynası.
--
-- Kafka "en az bir kez" teslim eder: tüketici işi yapıp offset'i commit edemeden ölürse (rebalance, deploy, OOM)
-- aynı mesaj tekrar gelir. Relay de Kafka'ya yazıp outbox satırını silemeden ölürse aynı olayı ikinci kez yayınlar.
-- Kural: mesajın etkisi ile inbox kaydı AYNI transaction'da yazılır. Ya ikisi birden kalıcı olur ya hiçbiri.
--
-- Anahtar Kafka offset'i DEĞİL, üreticinin verdiği event_id: aynı olay yeniden üretilirse (relay tekrarı) farklı
-- offset'e düşer ama event_id aynı kalır.
--
-- outcome:
--   APPLIED   etkisi uygulandı
--   CONFLICT  olay mevcut durumla çelişiyor (ör. iptal edilmiş emre dolum). Tekrar denemek düzeltmez; kaydedilir ve
--             alarm üretir. Inbox sayesinde "aynı olayın ikinci teslimi" (zararsız) ile "gerçek çelişki" (para hatası)
--             birbirinden ayrılır.
-- =====================================================================================================================
CREATE TABLE inbox (
    consumer     TEXT        NOT NULL,
    message_id   UUID        NOT NULL,
    message_type TEXT        NOT NULL,
    outcome      TEXT        NOT NULL CHECK (outcome IN ('APPLIED', 'CONFLICT')),
    detail       TEXT,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- Saklama süresi temizliği ve "son çelişkiler" sorgusu için.
CREATE INDEX ix_inbox_received ON inbox (received_at);
CREATE INDEX ix_inbox_conflicts ON inbox (consumer, received_at) WHERE outcome = 'CONFLICT';
