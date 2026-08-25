-- Outbox drain support.
--
-- The outbox has been written to since 2026-04-09 and never read: the codebase
-- contained the INSERT and no claimant, no completion, no retention. By
-- 2026-08-25 it held 36,224 rows — exactly the proposition count — and grew
-- monotonically with every canonical write.
--
-- docs/DATA_MODEL.md always described it as a work queue claimed with
-- FOR UPDATE SKIP LOCKED, so this adds the index that claim pattern needs
-- rather than changing what the table means.

-- Claims scan for the oldest unfinished event. Partial, so the index covers
-- only outstanding work and stays small once retention runs.
CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (created_at, id)
    WHERE completed_at IS NULL;

-- Retention deletes completed rows by age.
CREATE INDEX IF NOT EXISTS outbox_completed_idx
    ON outbox (completed_at)
    WHERE completed_at IS NOT NULL;
