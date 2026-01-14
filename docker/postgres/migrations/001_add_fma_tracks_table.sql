-- Migration: Add FMA tracks metadata table
-- This migration adds a new table to store FMA track metadata
-- It can be applied to an existing database without affecting other tables

-- Create FMA Track metadata table if it doesn't exist
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

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fma_title ON fma_tracks(title);
CREATE INDEX IF NOT EXISTS idx_fma_artist ON fma_tracks(artist);
CREATE INDEX IF NOT EXISTS idx_fma_genre ON fma_tracks(genre);
CREATE INDEX IF NOT EXISTS idx_fma_created ON fma_tracks(created_at);

-- Create trigger function if it doesn't exist (for other tables too)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for fma_tracks if it doesn't exist
DROP TRIGGER IF EXISTS update_fma_tracks_updated_at ON fma_tracks;
CREATE TRIGGER update_fma_tracks_updated_at
    BEFORE UPDATE ON fma_tracks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
