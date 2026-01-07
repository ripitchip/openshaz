"""Database operations module for PostgreSQL using SQLAlchemy."""

import os
import re
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from loguru import logger
from models.database import Base, OpensourceSong, QuerySong
from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

POSTGRES_USER = os.getenv("POSTGRES_USER", "music")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "musicpass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "musicdb")
POSTGRES_ADDRESS = os.getenv("POSTGRES_ADDRESS", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_ADDRESS}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

engine = create_engine(
    POSTGRES_URL,
    poolclass=QueuePool,
    pool_size=5,  # Number of persistent connections
    max_overflow=10,  # Max temporary connections
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=3600,  # Recycle connections after 1 hour
    echo=False,  # Set to True for SQL query logging
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_database():
    """Initialize database by creating all tables if they don't exist."""
    try:
        logger.info("Initializing database schema...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database schema initialized successfully")
    except SQLAlchemyError as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


@contextmanager
def get_db_session():
    """Context manager for database sessions with automatic cleanup.

    Usage:
        with get_db_session() as session:
            session.add(song)
            session.commit()
    """
    session = SessionLocal()
    try:
        yield session
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        session.close()


def extract_id_from_filename(filename: str) -> Optional[int]:
    """Extract integer ID from filename (e.g., '00002.mp3' -> 2).

    :param filename: Filename to parse
    :return: Extracted ID or None if not found
    """
    basename = os.path.splitext(os.path.basename(filename))[0]
    match = re.match(r"(\d+)", basename)
    if match:
        return int(match.group(1))
    logger.warning(f"Could not extract ID from filename: {filename}")
    return None


def store_opensource_song(
    name: str, bucket_url: str, features: List[float], song_id: Optional[int] = None
) -> OpensourceSong:
    """Store an opensource song with extracted features.

    :param name: Song name (filename)
    :param bucket_url: Object storage URL
    :param features: Feature vector
    :param song_id: Optional song ID; if None, extracted from name
    :return: Created OpensourceSong instance
    :raises IntegrityError: If song with same ID already exists
    """
    if song_id is None:
        song_id = extract_id_from_filename(name)
        if song_id is None:
            raise ValueError(f"Could not extract ID from filename: {name}")

    with get_db_session() as session:
        # Check if song already exists
        existing = session.query(OpensourceSong).filter_by(id=song_id).first()
        if existing:
            logger.info(f"Opensource song already exists: {name} (id={song_id})")
            return existing
        
        song = OpensourceSong(
            id=song_id, name=name, bucket_url=bucket_url, features=features
        )
        session.add(song)
        session.commit()
        session.refresh(song)
        logger.info(f"Stored opensource song: {name} (id={song.id})")
        return song


def store_query_song(
    name: str, bucket_url: str, features: List[float], song_id: Optional[int] = None
) -> QuerySong:
    """Store a query song with extracted features.

    :param name: Song name (filename)
    :param bucket_url: Object storage URL
    :param features: Feature vector
    :param song_id: Optional song ID; if None, extracted from name
    :return: Created QuerySong instance
    :raises IntegrityError: If song with same ID already exists
    """
    if song_id is None:
        song_id = extract_id_from_filename(name)
        if song_id is None:
            raise ValueError(f"Could not extract ID from filename: {name}")

    with get_db_session() as session:
        # Check if song already exists
        existing = session.query(QuerySong).filter_by(id=song_id).first()
        if existing:
            logger.info(f"Query song already exists: {name} (id={song_id})")
            return existing
        
        song = QuerySong(
            id=song_id, name=name, bucket_url=bucket_url, features=features
        )
        session.add(song)
        session.commit()
        session.refresh(song)
        logger.info(f"Stored query song: {name} (id={song.id})")
        return song


def get_all_opensource_songs() -> List[Dict[str, Any]]:
    """Fetch all opensource songs with their features.

    :return: List of dictionaries with song data
    """
    with get_db_session() as session:
        songs = session.execute(select(OpensourceSong)).scalars().all()
        result = [
            {
                "id": song.id,  # Now an integer
                "name": song.name,
                "bucket_url": song.bucket_url,
                "features": song.features,
            }
            for song in songs
        ]
        logger.info(f"Fetched {len(result)} opensource songs from database")
        return result


def get_opensource_song_by_name(name: str) -> Optional[OpensourceSong]:
    """Get an opensource song by name.

    :param name: Song name to search for
    :return: OpensourceSong instance or None
    """
    with get_db_session() as session:
        song = (
            session.execute(select(OpensourceSong).where(OpensourceSong.name == name))
            .scalars()
            .first()
        )
        return song


def get_query_song_by_name(name: str) -> Optional[QuerySong]:
    """Get a query song by name.

    :param name: Song name to search for
    :return: QuerySong instance or None
    """
    with get_db_session() as session:
        song = (
            session.execute(select(QuerySong).where(QuerySong.name == name))
            .scalars()
            .first()
        )
        return song


def count_opensource_songs() -> int:
    """Count total opensource songs in database.

    :return: Number of songs
    """
    with get_db_session() as session:
        count = session.query(OpensourceSong).count()
        return count


def count_query_songs() -> int:
    """Count total query songs in database.

    :return: Number of songs
    """
    with get_db_session() as session:
        count = session.query(QuerySong).count()
        return count


def health_check() -> bool:
    """Check if database connection is healthy.

    :return: True if healthy, False otherwise
    """
    try:
        with get_db_session() as session:
            session.execute(select(1))
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


def wipe_opensource_songs() -> int:
    """Delete all records from opensource_songs table.

    :return: Number of records deleted
    """
    with get_db_session() as session:
        count = session.query(OpensourceSong).count()
        session.query(OpensourceSong).delete()
        session.commit()
        logger.warning(f"Wiped {count} records from opensource_songs table")
        return count


def wipe_query_songs() -> int:
    """Delete all records from query_songs table.

    :return: Number of records deleted
    """
    with get_db_session() as session:
        count = session.query(QuerySong).count()
        session.query(QuerySong).delete()
        session.commit()
        logger.warning(f"Wiped {count} records from query_songs table")
        return count


def wipe_all_tables() -> Dict[str, int]:
    """Delete all records from both tables.

    :return: Dictionary with counts for each table
    """
    opensource_count = wipe_opensource_songs()
    query_count = wipe_query_songs()
    logger.warning(
        f"Wiped all tables: {opensource_count} opensource, {query_count} query songs"
    )
    return {"opensource_songs": opensource_count, "query_songs": query_count}
