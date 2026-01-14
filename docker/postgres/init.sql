-- FMA Track metadata table
CREATE TABLE IF NOT EXISTS fma_tracks (
    id INTEGER PRIMARY KEY,
    title VARCHAR(512) NOT NULL,
    artist VARCHAR(512),
    album VARCHAR(512),
    genre VARCHAR(128),
    listens INTEGER DEFAULT 0,
    year_created INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Opensource songs table (training data)
CREATE TABLE IF NOT EXISTS opensource_songs (
    id INTEGER PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    bucket_url TEXT NOT NULL,
    features JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Query songs table (test/query data)
CREATE TABLE IF NOT EXISTS query_songs (
    id INTEGER PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    bucket_url TEXT NOT NULL,
    features JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fma_title ON fma_tracks(title);
CREATE INDEX IF NOT EXISTS idx_fma_artist ON fma_tracks(artist);
CREATE INDEX IF NOT EXISTS idx_fma_genre ON fma_tracks(genre);
CREATE INDEX IF NOT EXISTS idx_fma_created ON fma_tracks(created_at);
CREATE INDEX IF NOT EXISTS idx_opensource_name ON opensource_songs(name);
CREATE INDEX IF NOT EXISTS idx_opensource_created ON opensource_songs(created_at);
CREATE INDEX IF NOT EXISTS idx_query_name ON query_songs(name);
CREATE INDEX IF NOT EXISTS idx_query_created ON query_songs(created_at);

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to auto-update updated_at
CREATE TRIGGER update_fma_tracks_updated_at
    BEFORE UPDATE ON fma_tracks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_opensource_songs_updated_at
    BEFORE UPDATE ON opensource_songs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_query_songs_updated_at
    BEFORE UPDATE ON query_songs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

