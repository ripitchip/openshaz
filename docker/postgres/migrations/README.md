# FMA Tracks Metadata Migration

This directory contains scripts and documentation for migrating FMA track metadata into the database.

## Overview

The migration adds a new `fma_tracks` table to store FMA metadata including:
- Track ID
- Title
- Artist
- Album
- Genre
- Listens count
- Year created

## Files

- `001_add_fma_tracks_table.sql` - SQL migration to create the fma_tracks table
- `../populate_fma_tracks.py` - Python script to extract and populate metadata

## Migration Steps

### For New Installations

The `fma_tracks` table is automatically created when running `init.sql`:

```bash
docker-compose up postgres
```

### For Existing Databases

1. **Apply the migration:**

```bash
psql -h localhost -U music -d musicdb -f migrations/001_add_fma_tracks_table.sql
```

Or if using Docker:

```bash
docker exec openshaz-postgres psql -U music -d musicdb -f /docker-entrypoint-initdb.d/migrations/001_add_fma_tracks_table.sql
```

2. **Populate the data:**

```bash
python script/populate_fma_tracks.py \
  --csv-path docker/app/worker/data/raw/fma_metadata/tracks.csv
```

With options:
- `--limit N` - Only process first N tracks
- `--batch-size N` - Commit every N records (default: 100)
- `--verify-only` - Verify data without inserting

## Usage

### Extract and Verify

```bash
python script/populate_fma_tracks.py \
  --csv-path docker/app/worker/data/raw/fma_metadata/tracks.csv \
  --verify-only \
  --limit 10
```

### Populate Full Database

```bash
python script/populate_fma_tracks.py \
  --csv-path docker/app/worker/data/raw/fma_metadata/tracks.csv
```

### With Custom Database

```bash
export POSTGRES_USER=myuser
export POSTGRES_PASSWORD=mypass
export POSTGRES_DB=mydb
export POSTGRES_ADDRESS=myhost
export POSTGRES_PORT=5432

python script/populate_fma_tracks.py \
  --csv-path path/to/tracks.csv
```

## Database Schema

```sql
CREATE TABLE fma_tracks (
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
```

## Query Examples

```sql
-- Get track metadata by ID
SELECT * FROM fma_tracks WHERE id = 2;

-- Find tracks by artist
SELECT * FROM fma_tracks WHERE artist LIKE '%Artist Name%';

-- Get tracks by genre
SELECT COUNT(*) as count, genre FROM fma_tracks GROUP BY genre;

-- Most listened tracks
SELECT title, artist, listens FROM fma_tracks 
ORDER BY listens DESC LIMIT 10;
```

## Notes

- The script skips records that already exist in the database
- Invalid data (missing required fields) is logged and skipped
- Dates are parsed and year is extracted automatically
- Null values are properly handled for optional fields
