-- Booking Detection Pipeline — Supabase Tables
-- ================================================
-- Creates three tables: input (domains to process), output (results), log (audit trail).
-- You can rename these tables — just update the --source-table, --dest-table, --log-table CLI args.

-- Auto-update trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------------
-- 1. Input table: domains to process
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS booking_detection_input (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain          TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'processing', 'done', 'error')),
    error_message   TEXT,
    batch_id        TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bdi_status ON booking_detection_input (status);
CREATE INDEX IF NOT EXISTS idx_bdi_batch  ON booking_detection_input (batch_id);

CREATE TRIGGER set_bdi_updated_at
    BEFORE UPDATE ON booking_detection_input
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- 2. Output table: one result row per domain (JSONB)
-- ---------------------------------------------------------------------------
-- result shape: {
--   "has_booking": true/false,
--   "booking_platform": "FareHarbor" | null,
--   "reasoning": "...",
--   "source_pass": "llm_html" | "regex_homepage" | ...,
--   "per_pass_metadata": { "pass1_scrape": {...}, "pass2_llm": {...}, ... }
-- }
CREATE TABLE IF NOT EXISTS booking_detection_output (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain          TEXT NOT NULL UNIQUE,
    result          JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_pass       TEXT,
    completed       BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bdo_domain    ON booking_detection_output (domain);
CREATE INDEX IF NOT EXISTS idx_bdo_completed ON booking_detection_output (completed);

CREATE TRIGGER set_bdo_updated_at
    BEFORE UPDATE ON booking_detection_output
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- 3. Log table: append-only audit trail
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS booking_detection_log (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain              TEXT,
    pass_name           TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    api_service         TEXT,
    request_preview     TEXT,
    response_preview    TEXT,
    http_status         INT,
    response_time_ms    INT,
    success             BOOLEAN,
    error_code          TEXT,
    error_message       TEXT,
    metadata            JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bdl_domain ON booking_detection_log (domain);
CREATE INDEX IF NOT EXISTS idx_bdl_pass   ON booking_detection_log (pass_name);
CREATE INDEX IF NOT EXISTS idx_bdl_event  ON booking_detection_log (event_type);
