CREATE TABLE IF NOT EXISTS processes (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    s3_key       TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    status       VARCHAR(30) NOT NULL DEFAULT 'RECEBIDO',
    report_id    TEXT NOT NULL DEFAULT '',
    error_msg    TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processes_status ON processes(status);
