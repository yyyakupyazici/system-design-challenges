-- Plan varsayılanları + challenge'daki örnek kurallar + demo API key'ler.

INSERT INTO rate_limit_policy (scope_type, scope_id, endpoint, rate_per_sec, burst, daily_quota, updated_by) VALUES
    ('PLAN',   'FREE',       '*',               10,   10,   50000, 'seed'),  -- Free: burst = rate (Yakup'un kararı)
    ('PLAN',   'FREE',       'POST /payments',   5,    5,   NULL,  'seed'),
    ('PLAN',   'PRO',        '*',              200,  400,   NULL,  'seed'),
    ('PLAN',   'PRO',        'POST /payments',  50,  100,   NULL,  'seed'),  -- yazma endpoint'i daha sıkı
    ('PLAN',   'ENTERPRISE', '*',             1000, 2000,   NULL,  'seed'),
    ('TENANT', 'tenant_42',  '*',              500, 1000,   NULL,  'seed');  -- tenant_42'ye özel anlaşma

-- Düz metin key'ler (sadece demo!): demo-free-key, demo-pro-key, demo-ent-key
INSERT INTO api_key (key_hash, tenant_id, plan) VALUES
    ('9f8cfdb17116bf0e2a44e071454f25594cb9b8737bf331e6a2f0b30550572f29', 'tenant_free_1', 'FREE'),
    ('1fbfd5e2a34698deb796eb78add256a067ec065519b8329b1eb85f5e2ef906d4', 'tenant_7',      'PRO'),
    ('558ef90b2b251ca38395db0d861f2b7e1615fb24e4fb73b1bcd5725ddd219e81', 'tenant_42',     'PRO');
