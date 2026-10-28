
import os
import re

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from storage_node.storage import ChunkStorage


NODE_ID = os.getenv("NODE_ID", "node1")
DATA_DIR = os.getenv("DATA_DIR", f"data/{NODE_ID}")

storage = ChunkStorage(DATA_DIR)

app = FastAPI(
    title=f"Distributed Storage - {NODE_ID}",
    version="1.0.0"
)


def validate_hash(chunk_hash: str):
    if not re.fullmatch(r"[0-9a-f]{64}", chunk_hash):
        raise HTTPException(
            status_code=400,
            detail="Chunk ID must be a lowercase SHA-256 hash"
        )


@app.get("/")
def root():
    return {
        "message": "Distributed Storage Node API",
        "node_id": NODE_ID
    }


@app.put("/chunks/{chunk_hash}")
async def upload_chunk(chunk_hash: str, request: Request):
    validate_hash(chunk_hash)

    try:
        result = await storage.save_chunk(
            chunk_hash,
            request.stream()
        )

        return {
            "node_id": NODE_ID,
            **result
        }

    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail=str(error)
        )


@app.get("/chunks/{chunk_hash}")
def download_chunk(chunk_hash: str):
    validate_hash(chunk_hash)

    path = storage.chunk_path(chunk_hash)

    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail="Chunk not found"
        )

    if not storage.verify_chunk(chunk_hash):
        raise HTTPException(
            status_code=409,
            detail="Chunk corrupted: SHA-256 verification failed"
        )

    return FileResponse(
        path=path,
        media_type="application/octet-stream",
        filename=chunk_hash
    )


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    validate_hash(chunk_hash)

    deleted = storage.delete_chunk(chunk_hash)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail="Chunk not found"
        )

    return {
        "node_id": NODE_ID,
        "chunk_hash": chunk_hash,
        "status": "deleted"
    }


@app.get("/heartbeat")
def heartbeat():
    return {
        "node_id": NODE_ID,
        "status": "healthy",
        **storage.get_stats()
    }


@app.get("/health")
def health():
    return {
        "node_id": NODE_ID,
        "status": "ok"
    }
