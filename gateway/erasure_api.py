
import hashlib
import os
import tempfile
from pathlib import Path

import requests
from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    UploadFile
)
from fastapi.background import BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from erasure.coding import ErasureCoder
from metadata.chunking import CHUNK_SIZE
from metadata.database import get_db
from metadata.models import (
    ErasureObject,
    ErasureStripe,
    ErasureFragment
)
from metadata.placement import NODE_URLS, create_ring


router = APIRouter(prefix="/erasure", tags=["Erasure Coding"])

coder = ErasureCoder()
ring = create_ring()
TIMEOUT = 20


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def put_fragment(node_id, fragment_hash, data):
    response = requests.put(
        f"{NODE_URLS[node_id]}/chunks/{fragment_hash}",
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT
    )
    response.raise_for_status()


def get_fragment(node_id, fragment_hash):
    try:
        response = requests.get(
            f"{NODE_URLS[node_id]}/chunks/{fragment_hash}",
            timeout=TIMEOUT
        )
        response.raise_for_status()

        data = response.content

        if sha256(data) != fragment_hash:
            return None

        return data

    except (requests.RequestException, KeyError):
        return None


def load_object(db, object_id):
    obj = db.scalar(
        select(ErasureObject)
        .options(
            selectinload(ErasureObject.stripes)
            .selectinload(ErasureStripe.fragments)
        )
        .where(ErasureObject.id == object_id)
    )

    if obj is None:
        raise HTTPException(404, "Erasure-coded object not found")

    return obj


@router.post("/objects")
def upload_erasure_object(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    filename = Path(file.filename or "unnamed").name

    if len(filename) > 255:
        raise HTTPException(400, "Filename too long")

    full_hasher = hashlib.sha256()
    total_size = 0
    stripe_count = 0

    obj = ErasureObject(
        filename=filename,
        file_hash="",
        size=0
    )

    try:
        db.add(obj)
        db.flush()

        while True:
            data = file.file.read(CHUNK_SIZE)

            if not data:
                break

            full_hasher.update(data)
            total_size += len(data)

            encoded = coder.encode(data)

            stripe = ErasureStripe(
                object_id=obj.id,
                position=stripe_count,
                original_hash=encoded["original_hash"],
                original_size=encoded["original_size"]
            )

            db.add(stripe)
            db.flush()

            # Choose three different nodes for the stripe.
            node_ids = ring.get_nodes(
                encoded["original_hash"],
                replica_count=3
            )

            for fragment, node_id in zip(
                encoded["fragments"], node_ids
            ):
                try:
                    put_fragment(
                        node_id,
                        fragment["hash"],
                        fragment["data"]
                    )

                except requests.RequestException as exc:
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            f"Failed storing fragment "
                            f"{fragment['index']} on {node_id}: {exc}"
                        )
                    )

                db.add(
                    ErasureFragment(
                        stripe_id=stripe.id,
                        fragment_index=fragment["index"],
                        fragment_hash=fragment["hash"],
                        node_id=node_id,
                        size=fragment["size"]
                    )
                )

            stripe_count += 1

        obj.file_hash = full_hasher.hexdigest()
        obj.size = total_size

        db.commit()

        return {
            "message": "Erasure-coded object uploaded",
            "object_id": obj.id,
            "filename": filename,
            "size_bytes": total_size,
            "file_hash": obj.file_hash,
            "stripe_count": stripe_count,
            "coding": "XOR 2+1"
        }

    except Exception:
        db.rollback()
        raise

    finally:
        file.file.close()


@router.get("/objects")
def list_erasure_objects(
    db: Session = Depends(get_db)
):
    objects = db.scalars(
        select(ErasureObject).order_by(ErasureObject.id)
    ).all()

    return [
        {
            "id": obj.id,
            "filename": obj.filename,
            "size_bytes": obj.size,
            "file_hash": obj.file_hash,
            "mode": "erasure_2_1"
        }
        for obj in objects
    ]


@router.get("/objects/{object_id}/metadata")
def erasure_metadata(
    object_id: int,
    db: Session = Depends(get_db)
):
    obj = load_object(db, object_id)

    return {
        "object_id": obj.id,
        "filename": obj.filename,
        "file_hash": obj.file_hash,
        "size_bytes": obj.size,
        "stripes": [
            {
                "position": stripe.position,
                "original_hash": stripe.original_hash,
                "original_size": stripe.original_size,
                "fragments": [
                    {
                        "index": f.fragment_index,
                        "hash": f.fragment_hash,
                        "node_id": f.node_id,
                        "size": f.size
                    }
                    for f in sorted(
                        stripe.fragments,
                        key=lambda x: x.fragment_index
                    )
                ]
            }
            for stripe in obj.stripes
        ]
    }


@router.get("/objects/{object_id}")
def download_erasure_object(
    object_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    obj = load_object(db, object_id)

    # Reconstruct into a temporary file before returning
    # HTTP 200, so missing fragments produce a real error.
    temp_path = None
    full_hasher = hashlib.sha256()

    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            suffix=".download"
        ) as output:
            temp_path = output.name

            for stripe in obj.stripes:
                available = {}

                for fragment in stripe.fragments:
                    data = get_fragment(
                        fragment.node_id,
                        fragment.fragment_hash
                    )

                    if data is not None:
                        available[fragment.fragment_index] = data

                if len(available) < 2:
                    raise HTTPException(
                        503,
                        f"Insufficient valid fragments "
                        f"for stripe {stripe.position}"
                    )

                try:
                    restored = coder.decode(
                        available,
                        stripe.original_size,
                        stripe.original_hash
                    )

                except ValueError as exc:
                    raise HTTPException(
                        503,
                        f"Reconstruction failed: {exc}"
                    )

                output.write(restored)
                full_hasher.update(restored)

        if full_hasher.hexdigest() != obj.file_hash:
            raise HTTPException(
                503, "Complete file hash mismatch"
            )

        background_tasks.add_task(os.unlink, temp_path)

        return FileResponse(
            path=temp_path,
            filename=obj.filename,
            media_type="application/octet-stream",
            background=background_tasks
        )

    except Exception:
        if temp_path is not None:
            Path(temp_path).unlink(missing_ok=True)
        raise
