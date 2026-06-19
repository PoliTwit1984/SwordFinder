-- Feedback + Public Roadmap table for SwordFinder
--
-- Stores in-app feature requests and bug reports submitted from the floating
-- Feedback launcher, plus the operator triage state that powers the public
-- roadmap (planned / shipped / rejected).
--
-- Privacy: contact_email and admin_notes are operator-only. The public roadmap
-- API never selects them; only request_type, message/public_title, status,
-- rejection_reason (rejected items only), and timestamps are exposed.

-- DROP TABLE IF EXISTS feedback CASCADE;  -- BE CAREFUL

CREATE TABLE IF NOT EXISTS feedback (
    id BIGSERIAL PRIMARY KEY,

    -- Submission
    request_type VARCHAR(20) NOT NULL DEFAULT 'feature'
        CHECK (request_type IN ('feature', 'bug')),
    message TEXT NOT NULL,

    -- Optional private contact + captured page context
    contact_email TEXT,
    page_path TEXT,
    page_url TEXT,
    user_agent TEXT,
    theme VARCHAR(40),

    -- Operator triage / roadmap state
    status VARCHAR(20) NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'planned', 'shipped', 'rejected')),
    rejection_reason TEXT,   -- shown publicly for rejected items
    admin_notes TEXT,        -- operator-only, never exposed publicly
    public_title VARCHAR(140),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for the roadmap (status + recency) and operator review (created_at).
CREATE INDEX IF NOT EXISTS idx_feedback_status ON feedback(status);
CREATE INDEX IF NOT EXISTS idx_feedback_request_type ON feedback(request_type);
CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON feedback(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_status_updated ON feedback(status, updated_at DESC);

-- Sample queries:
/*
-- Public roadmap source (no private columns):
SELECT id, request_type, message, public_title, status, rejection_reason, updated_at
FROM feedback
WHERE status IN ('planned', 'shipped', 'rejected')
ORDER BY updated_at DESC;

-- Operator inbox of untriaged requests:
SELECT * FROM feedback WHERE status = 'new' ORDER BY created_at DESC;
*/
