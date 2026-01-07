"""SQLAlchemy database models for OpenShaz music features storage."""

from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Index, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OpensourceSong(Base):
    """Table for opensource/training songs with extracted features."""

    __tablename__ = "opensource_songs"

    id = Column(Integer, primary_key=True)
    name = Column(String(512), nullable=False, index=True)
    bucket_url = Column(Text, nullable=False)
    features = Column(JSON, nullable=False)  # Stores feature vector as JSON array
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_opensource_name", "name"),
        Index("idx_opensource_created", "created_at"),
    )

    def __repr__(self):
        return f"<OpensourceSong(id={self.id}, name={self.name})>"


class QuerySong(Base):
    """Table for query/test songs with extracted features."""

    __tablename__ = "query_songs"

    id = Column(Integer, primary_key=True)
    name = Column(String(512), nullable=False, index=True)
    bucket_url = Column(Text, nullable=False)
    features = Column(JSON, nullable=False)  # Stores feature vector as JSON array
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_query_name", "name"),
        Index("idx_query_created", "created_at"),
    )

    def __repr__(self):
        return f"<QuerySong(id={self.id}, name={self.name})>"
