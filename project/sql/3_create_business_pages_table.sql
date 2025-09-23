CREATE TABLE business_pages (
    id SERIAL PRIMARY KEY,
    business_id TEXT REFERENCES businesses(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    page_type VARCHAR(50) DEFAULT 'Other',
    summary TEXT,
    email TEXT,
    page_speed_score INT,
    time_to_interactive_ms INT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(business_id, url)
);