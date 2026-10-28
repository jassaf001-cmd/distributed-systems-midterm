
import hashlib
import logging
import time

import requests
from sqlalchemy import select

from metadata.database import SessionLocal
from metadata.models import Chunk, ChunkReplica
from metadata.placement import NODE_URLS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("recovery")

REPLICA_COUNT = 3
CHECK_INTERVAL = 10
TIMEOUT = 3


def get_healthy_nodes():
    """Identify storage nodes responding to heartbeat."""
    healthy = set()

    for node_id, base_url in NODE_URLS.items():
        try:
            response = requests.get(
                f"{base_url}/heartbeat",
                timeout=TIMEOUT
            )
            response.raise_for_status()

            if response.json().get("node_id") == node_id:
                healthy.add(node_id)

        except (requests.RequestException, ValueError):
            logger.warning("Node unavailable: %s", node_id)

    return healthy


def fetch_valid_chunk(node_id, chunk_hash):
    """Fetch a replica and validate its SHA-256."""
    try:
        response = requests.get(
            f"{NODE_URLS[node_id]}/chunks/{chunk_hash}",
            timeout=TIMEOUT
        )
        response.raise_for_status()

        data = response.content
        actual_hash = hashlib.sha256(data).hexdigest()

        if actual_hash == chunk_hash:
            return data

        logger.warning(
            "Corrupted replica: %s on %s",
            chunk_hash[:12], node_id
        )

    except requests.RequestException as exc:
        logger.warning(
            "Cannot read chunk %s from %s: %s",
            chunk_hash[:12], node_id, exc
        )

    return None


def write_replica(node_id, chunk_hash, data):
    """Write a replacement replica to a healthy node."""
    try:
        response = requests.put(
            f"{NODE_URLS[node_id]}/chunks/{chunk_hash}",
            data=data,
            headers={
                "Content-Type": "application/octet-stream"
            },
            timeout=TIMEOUT
        )
        response.raise_for_status()
        return True

    except requests.RequestException as exc:
        logger.error(
            "Failed writing replica to %s: %s",
            node_id, exc
        )
        return False


def repair_chunk(db, chunk, healthy_nodes):
    chunk_hash = chunk.hash

    replicas = db.scalars(
        select(ChunkReplica).where(
            ChunkReplica.chunk_hash == chunk_hash
        )
    ).all()

    valid_nodes = set()
    source_data = None

    # Verify every recorded replica, not just node heartbeat.
    for replica in replicas:
        if replica.node_id not in healthy_nodes:
            continue

        data = fetch_valid_chunk(
            replica.node_id, chunk_hash
        )

        if data is not None:
            valid_nodes.add(replica.node_id)
            if source_data is None:
                source_data = data

    if source_data is None:
        logger.error(
            "DATA AT RISK: no valid copy of chunk %s",
            chunk_hash
        )
        return "unrecoverable"

    # Ensure the target replica count is attainable.
    desired_count = min(
        REPLICA_COUNT,
        len(healthy_nodes)
    )

    # Prefer node order for deterministic selection.
    for node_id in NODE_URLS:
        if len(valid_nodes) >= desired_count:
            break

        if node_id not in healthy_nodes:
            continue

        if node_id in valid_nodes:
            continue

        if write_replica(node_id, chunk_hash, source_data):
            valid_nodes.add(node_id)
            logger.info(
                "Repaired %s on %s",
                chunk_hash[:12], node_id
            )

    if len(valid_nodes) < REPLICA_COUNT:
        logger.warning(
            "Under-replicated chunk %s: %d/%d",
            chunk_hash[:12],
            len(valid_nodes),
            REPLICA_COUNT
        )

    # Update metadata to represent only verified replicas.
    # Do not remove metadata if the chunk had no recoverable source.
    for replica in replicas:
        if replica.node_id not in valid_nodes:
            db.delete(replica)

    recorded = {
        replica.node_id for replica in replicas
    }

    for node_id in valid_nodes - recorded:
        db.add(
            ChunkReplica(
                chunk_hash=chunk_hash,
                node_id=node_id
            )
        )

    db.commit()
    return "healthy" if len(valid_nodes) >= REPLICA_COUNT else "degraded"


def run_repair_cycle():
    healthy_nodes = get_healthy_nodes()

    logger.info(
        "Healthy nodes: %s",
        sorted(healthy_nodes)
    )

    if not healthy_nodes:
        logger.error("No healthy nodes. Skipping repair.")
        return

    results = {
        "healthy": 0,
        "degraded": 0,
        "unrecoverable": 0,
        "errors": 0
    }

    with SessionLocal() as db:
        chunks = db.scalars(select(Chunk)).all()

        for chunk in chunks:
            try:
                status = repair_chunk(
                    db, chunk, healthy_nodes
                )
                results[status] += 1

            except Exception:
                db.rollback()
                results["errors"] += 1
                logger.exception(
                    "Repair error for chunk %s",
                    chunk.hash
                )

    logger.info("Repair cycle complete: %s", results)


if __name__ == "__main__":
    logger.info("Recovery worker started")

    while True:
        try:
            run_repair_cycle()
        except Exception:
            logger.exception("Unexpected recovery error")

        time.sleep(CHECK_INTERVAL)
