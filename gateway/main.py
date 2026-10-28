
import hashlib
from collections import Counter
from urllib.parse import quote

import requests
from fastapi import (
    Depends,
    FastAPI,
    File,
    HTTPException,
    UploadFile
)
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from gateway.erasure_api import router as erasure_router
from metadata.database import get_db
from metadata.models import (
    StoredObject,
    Chunk,
    ObjectChunk,
    ChunkReplica
)
from metadata.placement import NODE_URLS, create_ring
from metadata.chunking import CHUNK_SIZE


app = FastAPI(
    title="Distributed Object Storage Gateway",
    version="1.0.0"
)
app.include_router(erasure_router)
ring = create_ring()
REPLICA_COUNT = 3
REQUEST_TIMEOUT = 15


def store_replicas(chunk_hash: str, data: bytes):
    """Upload one chunk to three distinct nodes."""
    node_ids = ring.get_nodes(
        chunk_hash,
        REPLICA_COUNT
    )

    stored_nodes = []

    for node_id in node_ids:
        url = f"{NODE_URLS[node_id]}/chunks/{chunk_hash}"

        try:
            response = requests.put(
                url,
                data=data,
                headers={
                    "Content-Type": "application/octet-stream"
                },
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            stored_nodes.append(node_id)

        except requests.RequestException as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Replica upload failed on {node_id}: {exc}"
            )

    return stored_nodes


@app.get("/")
def home():
    return {
        "service": "Distributed Object Storage Gateway",
        "status": "running",
        "storage_nodes": len(NODE_URLS),
        "replication_factor": REPLICA_COUNT
    }


@app.post("/objects")
def upload_object(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    file_hasher = hashlib.sha256()
    total_size = 0
    position = 0

    obj = StoredObject(
        filename=file.filename or "unnamed",
        file_hash="",
        size=0
    )

    db.add(obj)
    db.flush()

    try:
        while True:
            data = file.file.read(CHUNK_SIZE)

            if not data:
                break

            file_hasher.update(data)
            total_size += len(data)

            chunk_hash = hashlib.sha256(
                data
            ).hexdigest()

            chunk = db.get(Chunk, chunk_hash)

            if chunk is None:
                # Only upload newly encountered chunks.
                nodes = store_replicas(
                    chunk_hash, data
                )

                chunk = Chunk(
                    hash=chunk_hash,
                    size=len(data),
                    reference_count=0
                )
                db.add(chunk)
                db.flush()

                for node_id in nodes:
                    db.add(
                        ChunkReplica(
                            chunk_hash=chunk_hash,
                            node_id=node_id
                        )
                    )

            # Count every logical occurrence.
            chunk.reference_count += 1

            db.add(
                ObjectChunk(
                    object_id=obj.id,
                    chunk_hash=chunk_hash,
                    position=position
                )
            )

            position += 1

        obj.file_hash = file_hasher.hexdigest()
        obj.size = total_size

        db.commit()
        db.refresh(obj)

        return {
            "message": "File uploaded successfully",
            "object_id": obj.id,
            "filename": obj.filename,
            "file_hash": obj.file_hash,
            "size_bytes": obj.size,
            "chunk_count": position,
            "replication_factor": REPLICA_COUNT
        }

    except Exception:
        db.rollback()
        raise

    finally:
        file.file.close()


@app.get("/objects")
def list_objects(db: Session = Depends(get_db)):
    objects = db.scalars(
        select(StoredObject).order_by(StoredObject.id)
    ).all()

    return [
        {
            "id": obj.id,
            "filename": obj.filename,
            "size_bytes": obj.size,
            "file_hash": obj.file_hash
        }
        for obj in objects
    ]


@app.get("/objects/{object_id}/metadata")
def object_metadata(
    object_id: int,
    db: Session = Depends(get_db)
):
    obj = db.get(StoredObject, object_id)

    if obj is None:
        raise HTTPException(404, "Object not found")

    parts = db.scalars(
        select(ObjectChunk)
        .where(ObjectChunk.object_id == object_id)
        .order_by(ObjectChunk.position)
    ).all()

    chunks = []

    for part in parts:
        replica_rows = db.scalars(
            select(ChunkReplica).where(
                ChunkReplica.chunk_hash == part.chunk_hash
            )
        ).all()

        chunks.append({
            "position": part.position,
            "hash": part.chunk_hash,
            "nodes": [r.node_id for r in replica_rows]
        })

    return {
        "object_id": obj.id,
        "filename": obj.filename,
        "file_hash": obj.file_hash,
        "size_bytes": obj.size,
        "chunks": chunks
    }


@app.get("/objects/{object_id}")
def download_object(
    object_id: int,
    db: Session = Depends(get_db)
):
    obj = db.get(StoredObject, object_id)

    if obj is None:
        raise HTTPException(404, "Object not found")

    parts = db.scalars(
        select(ObjectChunk)
        .where(ObjectChunk.object_id == object_id)
        .order_by(ObjectChunk.position)
    ).all()

    manifest = []

    for part in parts:
        replicas = db.scalars(
            select(ChunkReplica).where(
                ChunkReplica.chunk_hash == part.chunk_hash
            )
        ).all()

        manifest.append((
            part.chunk_hash,
            [r.node_id for r in replicas]
        ))

    def generate_file():
        complete_hash = hashlib.sha256()

        for chunk_hash, nodes in manifest:
            chunk_data = None

            for node_id in nodes:
                try:
                    response = requests.get(
                        f"{NODE_URLS[node_id]}/chunks/{chunk_hash}",
                        timeout=REQUEST_TIMEOUT
                    )
                    response.raise_for_status()

                    data = response.content

                    if hashlib.sha256(data).hexdigest() == chunk_hash:
                        chunk_data = data
                        break

                except requests.RequestException:
                    continue

            if chunk_data is None:
                raise RuntimeError(
                    f"No valid replica for chunk {chunk_hash}"
                )

            complete_hash.update(chunk_data)
            yield chunk_data

        if complete_hash.hexdigest() != obj_hash:
            raise RuntimeError("Complete file hash mismatch")

    obj_hash = obj.file_hash

    filename = quote(obj.filename)

    return StreamingResponse(
        generate_file(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition":
                f"attachment; filename*=UTF-8''{filename}"
        }
    )


@app.delete("/objects/{object_id}")
def delete_object(
    object_id: int,
    db: Session = Depends(get_db)
):
    obj = db.get(StoredObject, object_id)

    if obj is None:
        raise HTTPException(404, "Object not found")

    parts = db.scalars(
        select(ObjectChunk).where(
            ObjectChunk.object_id == object_id
        )
    ).all()

    counts = Counter(
        part.chunk_hash for part in parts
    )

    unused_chunks = []

    for chunk_hash, count in counts.items():
        chunk = db.get(Chunk, chunk_hash)

        chunk.reference_count -= count

        if chunk.reference_count == 0:
            replicas = db.scalars(
                select(ChunkReplica).where(
                    ChunkReplica.chunk_hash == chunk_hash
                )
            ).all()

            unused_chunks.append((
                chunk_hash,
                [r.node_id for r in replicas]
            ))

            db.delete(chunk)

    db.delete(obj)
    db.commit()

    failed_deletions = []

    # Remove physical chunks only after metadata commit.
    for chunk_hash, nodes in unused_chunks:
        for node_id in nodes:
            try:
                response = requests.delete(
                    f"{NODE_URLS[node_id]}/chunks/{chunk_hash}",
                    timeout=REQUEST_TIMEOUT
                )
                if response.status_code not in (200, 404):
                    failed_deletions.append(node_id)
            except requests.RequestException:
                failed_deletions.append(node_id)

    return {
        "message": "Object metadata deleted",
        "object_id": object_id,
        "physical_cleanup_complete": not failed_deletions,
        "failed_cleanup_nodes": failed_deletions
    }


@app.get("/cluster/health")
def cluster_health():
    result = {}

    for node_id, url in NODE_URLS.items():
        try:
            response = requests.get(
                f"{url}/heartbeat",
                timeout=3
            )
            response.raise_for_status()

            result[node_id] = {
                "status": "healthy",
                **response.json()
            }
        except requests.RequestException:
            result[node_id] = {
                "status": "unavailable"
            }

    return result
