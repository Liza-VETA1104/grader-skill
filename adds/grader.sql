CREATE TABLE grader_attempts (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    oauth_consumer_key VARCHAR(255),
    lis_result_sourcedid TEXT,
    lis_outcome_service_url TEXT,
    is_correct BOOLEAN,
    attempt_type VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE
);

-- Индексы для ускорения поиска
CREATE INDEX idx_user_id ON grader_attempts(user_id);
CREATE INDEX idx_created_at ON grader_attempts(created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_attempt 
ON grader_attempts(user_id, lis_result_sourcedid, created_at);
