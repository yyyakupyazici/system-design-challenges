-- Cüzdan olaylarını dinleme: ödeme sonradan iade edilebilir (chargeback, fraud iptali) → REVERSED.
ALTER TABLE payment_requests DROP CONSTRAINT payment_requests_status_check;
ALTER TABLE payment_requests ADD CONSTRAINT payment_requests_status_check
    CHECK (status IN ('PENDING', 'PROCESSING', 'PAID', 'DECLINED', 'EXPIRED', 'REVERSED'));
ALTER TABLE payment_requests ADD COLUMN reversal_reason text;

-- İade olayı transfer id ile gelir: hangi para isteğine ait olduğunu buradan buluruz.
CREATE UNIQUE INDEX payment_requests_transfer ON payment_requests (wallet_transfer_id) WHERE wallet_transfer_id IS NOT NULL;
