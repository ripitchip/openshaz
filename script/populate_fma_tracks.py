#!/usr/bin/env python3
"""
Script to extract FMA track metadata and populate the database.

This script reads the FMA metadata CSV and inserts all track information
into the fma_tracks table in the database.
"""

import argparse
import ast
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from loguru import logger

# Import SQLAlchemy after environment is set
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "music")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "musicpass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "musicdb")
POSTGRES_ADDRESS = os.getenv("POSTGRES_ADDRESS", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DB}"


def load_fma_tracks(filepath: str) -> pd.DataFrame:
    """Load FMA tracks CSV with proper handling of multi-level columns."""
    logger.info(f"Loading FMA metadata from: {filepath}")
    
    tracks = pd.read_csv(filepath, index_col=0, header=[0, 1])
    
    # Parse JSON columns
    json_columns = [
        ("track", "tags"),
        ("album", "tags"),
        ("artist", "tags"),
        ("track", "genres"),
        ("track", "genres_all"),
    ]
    for column in json_columns:
        if column in tracks.columns:
            tracks[column] = tracks[column].map(ast.literal_eval)
    
    # Convert date columns
    date_columns = [
        ("track", "date_created"),
        ("track", "date_recorded"),
        ("album", "date_created"),
        ("album", "date_released"),
        ("artist", "date_created"),
        ("artist", "active_year_begin"),
        ("artist", "active_year_end"),
    ]
    for column in date_columns:
        if column in tracks.columns:
            tracks[column] = pd.to_datetime(tracks[column], errors='coerce')
    
    logger.info(f"Loaded {len(tracks)} tracks from metadata")
    return tracks


def extract_track_metadata(tracks: pd.DataFrame) -> List[Dict]:
    """Extract required metadata from tracks DataFrame."""
    logger.info("Extracting track metadata...")
    
    metadata_list = []
    
    for track_id, row in tracks.iterrows():
        try:
            # Extract year from date_created
            year_created = None
            if ("track", "date_created") in tracks.columns:
                date_created = row[("track", "date_created")]
                if pd.notna(date_created):
                    year_created = int(date_created.year)
            
            metadata = {
                'id': int(track_id),
                'title': str(row[("track", "title")] if ("track", "title") in tracks.columns else "Unknown"),
                'artist': str(row[("artist", "name")] if ("artist", "name") in tracks.columns else None),
                'album': str(row[("album", "title")] if ("album", "title") in tracks.columns else None),
                'genre': str(row[("track", "genre_top")] if ("track", "genre_top") in tracks.columns else None),
                'listens': int(row[("track", "listens")] if ("track", "listens") in tracks.columns and pd.notna(row[("track", "listens")]) else 0),
                'year_created': year_created,
            }
            
            # Handle None values for optional fields
            if metadata['artist'] == 'None' or pd.isna(row[("artist", "name")] if ("artist", "name") in tracks.columns else None):
                metadata['artist'] = None
            if metadata['album'] == 'None' or pd.isna(row[("album", "title")] if ("album", "title") in tracks.columns else None):
                metadata['album'] = None
            if metadata['genre'] == 'None' or pd.isna(row[("track", "genre_top")] if ("track", "genre_top") in tracks.columns else None):
                metadata['genre'] = None
            
            metadata_list.append(metadata)
            
        except Exception as e:
            logger.warning(f"Error extracting metadata for track {track_id}: {e}")
            continue
    
    logger.info(f"Extracted metadata for {len(metadata_list)} tracks")
    return metadata_list


def insert_into_database(metadata_list: List[Dict], batch_size: int = 100) -> int:
    """Insert metadata into the database in batches."""
    logger.info(f"Connecting to database at {POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    engine = create_engine(POSTGRES_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        for i, metadata in enumerate(metadata_list, 1):
            try:
                # Check if track already exists
                result = session.execute(
                    text("SELECT id FROM fma_tracks WHERE id = :id"),
                    {"id": metadata['id']}
                )
                
                if result.fetchone():
                    logger.debug(f"Track {metadata['id']} already exists, skipping")
                    skipped_count += 1
                    continue
                
                # Insert new track
                insert_query = text("""
                    INSERT INTO fma_tracks 
                    (id, title, artist, album, genre, listens, year_created)
                    VALUES (:id, :title, :artist, :album, :genre, :listens, :year_created)
                """)
                
                session.execute(insert_query, metadata)
                inserted_count += 1
                
                # Commit in batches
                if i % batch_size == 0:
                    session.commit()
                    logger.info(f"Committed {i} records to database")
                    
            except Exception as e:
                logger.warning(f"Error inserting track {metadata['id']}: {e}")
                session.rollback()
                continue
        
        # Final commit for remaining records
        session.commit()
        logger.info("Final commit completed")
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        session.rollback()
        raise
    finally:
        session.close()
    
    return inserted_count, skipped_count


def main():
    parser = argparse.ArgumentParser(
        description="Extract FMA track metadata and populate the database"
    )
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to the FMA tracks.csv file"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of tracks to process (default: all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for database commits (default: 100)"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only load and verify data without inserting"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    
    try:
        # Load FMA metadata
        csv_path = Path(args.csv_path)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return 1
        
        tracks = load_fma_tracks(str(csv_path))
        
        # Limit if specified
        if args.limit:
            tracks = tracks.head(args.limit)
            logger.info(f"Limited to {len(tracks)} tracks")
        
        # Extract metadata
        metadata_list = extract_track_metadata(tracks)
        
        # Display sample
        logger.info("\nSample of extracted metadata:")
        for i, metadata in enumerate(metadata_list[:3]):
            logger.info(f"  Track {i+1}: {metadata}")
        
        # Insert into database
        if args.verify_only:
            logger.info("Verify-only mode: data not inserted")
            logger.info(f"Would insert {len(metadata_list)} tracks")
        else:
            logger.info(f"\nInserting {len(metadata_list)} tracks into database...")
            inserted, skipped = insert_into_database(metadata_list, args.batch_size)
            
            logger.info("\n--- Summary ---")
            logger.info(f"Inserted: {inserted}")
            logger.info(f"Skipped: {skipped}")
            logger.info(f"Total processed: {inserted + skipped}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
