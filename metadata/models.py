
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from metadata.database import Base


def utc_now():
    return datetime.now(timezone.utc)


class StoredObject(Base):
    __tablename__ = "objects"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True
    )
    filename: Mapped[str] = mapped_column(
        String(255), nullable=False
    )
    file_hash: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    size: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now
    )

    chunks = relationship(
        "ObjectChunk",
        back_populates="object",
        cascade="all, delete-orphan",
        order_by="ObjectChunk.position"
    )


class Chunk(Base):
    __tablename__ = "chunks"

    hash: Mapped[str] = mapped_column(
        String(64), primary_key=True
    )
    size: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )
    reference_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )

    replicas = relationship(
        "ChunkReplica",
        back_populates="chunk",
        cascade="all, delete-orphan"
    )


class ObjectChunk(Base):
    __tablename__ = "object_chunks"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True
    )
    object_id: Mapped[int] = mapped_column(
        ForeignKey("objects.id", ondelete="CASCADE")
    )
    chunk_hash: Mapped[str] = mapped_column(
        ForeignKey("chunks.hash")
    )
    position: Mapped[int] = mapped_column(
        Integer, nullable=False
    )

    object = relationship(
        "StoredObject", back_populates="chunks"
    )

    __table_args__ = (
        UniqueConstraint("object_id", "position"),
    )


class ChunkReplica(Base):
    __tablename__ = "chunk_replicas"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True
    )
    chunk_hash: Mapped[str] = mapped_column(
        ForeignKey("chunks.hash", ondelete="CASCADE")
    )
    node_id: Mapped[str] = mapped_column(
        String(100), nullable=False
    )

    chunk = relationship(
        "Chunk", back_populates="replicas"
    )

    __table_args__ = (
        UniqueConstraint("chunk_hash", "node_id"),
    )

class ErasureObject(Base):
    __tablename__ = "erasure_objects"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True
    )
    filename: Mapped[str] = mapped_column(
        String(255), nullable=False
    )
    file_hash: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    size: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now
    )

    stripes = relationship(
        "ErasureStripe",
        back_populates="object",
        cascade="all, delete-orphan",
        order_by="ErasureStripe.position"
    )


class ErasureStripe(Base):
    __tablename__ = "erasure_stripes"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True
    )
    object_id: Mapped[int] = mapped_column(
        ForeignKey("erasure_objects.id"),
        nullable=False
    )
    position: Mapped[int] = mapped_column(
        Integer, nullable=False
    )
    original_hash: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    original_size: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )

    object = relationship(
        "ErasureObject", back_populates="stripes"
    )
    fragments = relationship(
        "ErasureFragment",
        back_populates="stripe",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("object_id", "position"),
    )


class ErasureFragment(Base):
    __tablename__ = "erasure_fragments"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True
    )
    stripe_id: Mapped[int] = mapped_column(
        ForeignKey("erasure_stripes.id"),
        nullable=False
    )
    fragment_index: Mapped[int] = mapped_column(
        Integer, nullable=False
    )
    fragment_hash: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    node_id: Mapped[str] = mapped_column(
        String(100), nullable=False
    )
    size: Mapped[int] = mapped_column(
        BigInteger, nullable=False
    )

    stripe = relationship(
        "ErasureStripe", back_populates="fragments"
    )

    __table_args__ = (
        UniqueConstraint("stripe_id", "fragment_index"),
        UniqueConstraint("stripe_id", "node_id"),
    )
