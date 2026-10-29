-- Demo verisi: docker-compose ve testlerde kullanılır.
INSERT INTO app_user (user_id, phone, email, locale, time_zone) VALUES
    ('u_1', '+905550000001', 'ayse@example.com', 'tr', 'Europe/Istanbul'),
    ('u_2', '+905550000002', 'mehmet@example.com', 'tr', 'Europe/Istanbul'),
    ('u_3', '+447700900003', 'john@example.com', 'en', 'Europe/London');

INSERT INTO device_token (installation_id, user_id, platform, token) VALUES
    ('inst_u1_phone', 'u_1', 'IOS', 'apns-token-u1'),
    ('inst_u3_phone', 'u_3', 'ANDROID', 'fcm-token-u3');

-- u_1: pazarlama izni var. u_2: yok. u_3: SMS kapalı.
INSERT INTO user_preference (user_id, marketing_consent, sms_enabled) VALUES
    ('u_1', TRUE, TRUE),
    ('u_2', FALSE, TRUE),
    ('u_3', TRUE, FALSE);

INSERT INTO template (template_id, locale, channel, title, body) VALUES
    ('otp_login', 'tr', 'SMS',   NULL,                     'Paramix giriş kodun: {{code}}. Kimseyle paylaşma.'),
    ('otp_login', 'tr', 'PUSH',  'Giriş kodun',            '{{code}}'),
    ('otp_login', 'en', 'SMS',   NULL,                     'Your Paramix login code: {{code}}. Do not share it.'),
    ('otp_login', 'en', 'PUSH',  'Your login code',        '{{code}}'),

    ('tx_credited', 'tr', 'SMS',   NULL,                   '{{sender}} sana {{amount}} {{currency}} gönderdi.'),
    ('tx_credited', 'tr', 'PUSH',  'Hesabına para geldi',  '{{sender}} sana {{amount}} {{currency}} gönderdi.'),
    ('tx_credited', 'tr', 'EMAIL', 'Hesabına para geldi',  'Merhaba, {{sender}} sana {{amount}} {{currency}} gönderdi.'),
    ('tx_credited', 'en', 'SMS',   NULL,                   '{{sender}} sent you {{amount}} {{currency}}.'),
    ('tx_credited', 'en', 'PUSH',  'Money received',       '{{sender}} sent you {{amount}} {{currency}}.'),
    ('tx_credited', 'en', 'EMAIL', 'Money received',       'Hi, {{sender}} sent you {{amount}} {{currency}}.'),

    ('promo_generic', 'tr', 'SMS',   NULL,                 '{{headline}} Detay: {{url}} IYS ret: RET yazip 3434e gonder.'),
    ('promo_generic', 'tr', 'PUSH',  '{{headline}}',       '{{url}}'),
    ('promo_generic', 'tr', 'EMAIL', '{{headline}}',       '{{headline}} Detaylar: {{url}}'),
    ('promo_generic', 'en', 'SMS',   NULL,                 '{{headline}} More: {{url}} Reply STOP to opt out.'),
    ('promo_generic', 'en', 'PUSH',  '{{headline}}',       '{{url}}'),
    ('promo_generic', 'en', 'EMAIL', '{{headline}}',       '{{headline}} Details: {{url}}');

INSERT INTO campaign (campaign_id, name, status) VALUES
    ('cmp_demo', 'Demo kampanya', 'RUNNING');
