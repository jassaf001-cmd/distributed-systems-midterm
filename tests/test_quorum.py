
import hashlib
import pytest

from gateway.quorum import quorum_write, quorum_read
from gateway.quorum import QuorumError


def test_write_and_read_quorum():
    data = b"Distributed Systems Quorum Test"
    chunk_hash = hashlib.sha256(data).hexdigest()

    result = quorum_write(chunk_hash, data)

    assert result["quorum_reached"]
    assert len(result["successful_nodes"]) >= 2

    restored = quorum_read(
        chunk_hash,
        result["successful_nodes"]
    )

    assert restored == data


def test_invalid_hash_rejected():
    with pytest.raises(ValueError):
        quorum_write("0" * 64, b"incorrect content")


def test_read_without_replicas_fails():
    with pytest.raises(QuorumError):
        quorum_read("0" * 64, [])
