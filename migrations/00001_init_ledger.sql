-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pools (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    contribution_amount NUMERIC(20, 4) NOT NULL CHECK (contribution_amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    pool_id UUID REFERENCES pools(id) ON DELETE RESTRICT,
    type TEXT NOT NULL,
    description TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    account_id UUID NOT NULL,
    amount NUMERIC(20, 4) NOT NULL CHECK (amount <> 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ledger_entries_account_created_at_idx
    ON ledger_entries (account_id, created_at);

CREATE INDEX ledger_entries_transaction_id_idx
    ON ledger_entries (transaction_id);

CREATE OR REPLACE FUNCTION check_transaction_balanced()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    entry_total NUMERIC(20, 4);
BEGIN
    SELECT COALESCE(SUM(amount), 0.0000)
    INTO entry_total
    FROM ledger_entries
    WHERE transaction_id = NEW.transaction_id;

    IF entry_total <> 0.0000 THEN
        RAISE EXCEPTION 'transaction % is unbalanced: total %', NEW.transaction_id, entry_total;
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER ledger_entries_balance_check
AFTER INSERT ON ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION check_transaction_balanced();

-- +goose Down
DROP TRIGGER IF EXISTS ledger_entries_balance_check ON ledger_entries;
DROP FUNCTION IF EXISTS check_transaction_balanced();
DROP INDEX IF EXISTS ledger_entries_transaction_id_idx;
DROP INDEX IF EXISTS ledger_entries_account_created_at_idx;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS pools;
DROP TABLE IF EXISTS customers;
