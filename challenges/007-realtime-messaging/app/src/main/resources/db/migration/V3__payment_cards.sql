-- Para isteği artık payment-request-service'in (kendi veritabanı, kendi API'si). Sohbette sadece KART kalır:
-- servisin "payment.events" olaylarından kurulan, salt okunur bir projeksiyon (read model).
--
-- version: olaydaki sürüm. Kart sadece daha YENİ sürümle güncellenir; tekrar gelen ya da sırası karışan eski olay
-- hiçbir şey değiştirmez. Olay tam bir anlık görüntü (snapshot) taşıdığı için "ödendi" olayı "oluşturuldu"dan önce
-- gelse bile kart doğru kurulur.
CREATE TABLE payment_cards (
    payment_request_id bigint PRIMARY KEY,
    conversation_id    bigint NOT NULL REFERENCES conversations (id),
    requester_id       bigint NOT NULL,
    payer_id           bigint NOT NULL,
    amount_kurus       bigint NOT NULL,
    description        text   NOT NULL DEFAULT '',
    status             text   NOT NULL,
    version            int    NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- Demo projesi: eski tablo taşınmadan kaldırılıyor. Üretimde önce veri yeni servise kopyalanır (backfill),
-- iki taraf bir süre birlikte çalışır, sonra eski tablo silinir (expand → migrate → contract).
DROP TABLE payment_requests;
