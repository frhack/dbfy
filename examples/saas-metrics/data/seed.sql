-- Billing tables: customers + their MRR + actuals per month.

CREATE TABLE customers (
    id           BIGSERIAL PRIMARY KEY,
    slug         TEXT UNIQUE NOT NULL,  -- matches the slug returned by GraphQL
    name         TEXT NOT NULL,
    plan         TEXT NOT NULL,         -- 'starter' | 'pro' | 'enterprise'
    mrr_cents    INTEGER NOT NULL,
    contract_end DATE
);

CREATE TABLE monthly_revenue (
    customer_id  BIGINT NOT NULL REFERENCES customers(id),
    month        DATE NOT NULL,
    actual_cents INTEGER NOT NULL,
    PRIMARY KEY (customer_id, month)
);

INSERT INTO customers (slug, name, plan, mrr_cents, contract_end) VALUES
    ('acme',         'ACME Corp',          'enterprise', 50000, '2027-04-30'),
    ('globex',       'Globex',             'pro',        15000, '2026-09-30'),
    ('initech',      'Initech',            'pro',        15000, '2026-12-31'),
    ('soylent',      'Soylent',            'starter',     2500, '2026-06-30'),
    ('umbrella',     'Umbrella Corp',      'enterprise', 75000, '2027-04-30'),
    ('cyberdyne',    'Cyberdyne',          'pro',        20000, '2026-08-31'),
    ('tyrell',       'Tyrell',             'starter',     2500, '2026-05-31'),
    ('massive',      'Massive Dynamic',    'enterprise', 60000, '2027-04-30');

INSERT INTO monthly_revenue (customer_id, month, actual_cents) VALUES
    -- April 2026: most paid full MRR; some less
    (1, '2026-04-01', 50000),
    (2, '2026-04-01', 15000),
    (3, '2026-04-01', 12000),  -- Initech: paid less than MRR (drift)
    (4, '2026-04-01',  2500),
    (5, '2026-04-01', 75000),
    (6, '2026-04-01', 20000),
    (7, '2026-04-01',  2500),
    (8, '2026-04-01', 60000);
