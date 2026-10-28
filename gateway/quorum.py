
import hashlib
import requests

from metadata.placement import NODE_URLS, create_ring


REPLICA_COUNT = 3
WRITE_QUORUM = 2
READ_QUORUM = 2
TIMEOUT = 5

ring = create_ring()


class QuorumError(Exception):
    pass


def quorum_write(chunk_hash: str, data: bytes):
    if hashlib.sha256(data).hexdigest() != chunk_hash:
        raise ValueError("Invalid chunk checksum")

    target_nodes = ring.get_nodes(
        chunk_hash,
        REPLICA_COUNT
    )

    successful_nodes = []
    failed_nodes = []

    # Try all three replicas even after quorum is satisfied.
    for node_id in target_nodes:
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
            successful_nodes.append(node_id)

        except requests.RequestException:
            failed_nodes.append(node_id)

    if len(successful_nodes) < WRITE_QUORUM:
        raise QuorumError(
            f"Write quorum failed: "
            f"{len(successful_nodes)}/{WRITE_QUORUM}"
        )

    return {
        "successful_nodes": successful_nodes,
        "failed_nodes": failed_nodes,
        "acknowledgments": len(successful_nodes),
        "quorum_reached": True
    }


def quorum_read(chunk_hash: str, replica_nodes: list[str]):
    valid_responses = {}

    for node_id in dict.fromkeys(replica_nodes):
        if node_id not in NODE_URLS:
            continue

        try:
            response = requests.get(
                f"{NODE_URLS[node_id]}/chunks/{chunk_hash}",
                timeout=TIMEOUT
            )
            response.raise_for_status()

            data = response.content

            if hashlib.sha256(data).hexdigest() == chunk_hash:
                valid_responses[node_id] = data

        except requests.RequestException:
            continue

    if len(valid_responses) < READ_QUORUM:
        raise QuorumError(
            f"Read quorum failed: "
            f"{len(valid_responses)}/{READ_QUORUM}"
        )

    # Hash validation ensures all accepted responses
    # correspond to the requested immutable content.
    return next(iter(valid_responses.values()))
