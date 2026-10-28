
import os

import pytest

from erasure.coding import ErasureCoder


@pytest.fixture
def coder():
    return ErasureCoder()


@pytest.mark.parametrize("size", [
    0,
    1,
    7,
    1024,
    1024 * 1024,
    4 * 1024 * 1024,
])
def test_encode_decode(coder, size):
    original = os.urandom(size)

    encoded = coder.encode(original)

    fragments = {
        f["index"]: f["data"]
        for f in encoded["fragments"]
    }

    restored = coder.decode(
        fragments,
        encoded["original_size"],
        encoded["original_hash"]
    )

    assert restored == original


@pytest.mark.parametrize("missing_index", [0, 1, 2])
def test_single_fragment_recovery(coder, missing_index):
    original = os.urandom(4097)

    encoded = coder.encode(original)

    fragments = {
        f["index"]: f["data"]
        for f in encoded["fragments"]
    }

    expected_fragment = fragments[missing_index]
    del fragments[missing_index]

    restored_fragment = coder.repair(
        fragments, missing_index
    )

    assert restored_fragment == expected_fragment

    fragments[missing_index] = restored_fragment

    restored = coder.decode(
        fragments,
        encoded["original_size"],
        encoded["original_hash"]
    )

    assert restored == original


def test_two_missing_fragments_fail(coder):
    encoded = coder.encode(b"Distributed Systems")

    fragments = {
        0: encoded["fragments"][0]["data"]
    }

    with pytest.raises(ValueError):
        coder.decode(
            fragments,
            encoded["original_size"]
        )


def test_corrupted_fragment_detected(coder):
    original = b"Testing erasure coding integrity"

    encoded = coder.encode(original)

    fragments = {
        f["index"]: f["data"]
        for f in encoded["fragments"]
    }

    corrupted = bytearray(fragments[0])
    corrupted[0] ^= 0xFF
    fragments[0] = bytes(corrupted)

    with pytest.raises(ValueError):
        coder.decode(
            fragments,
            encoded["original_size"],
            encoded["original_hash"]
        )
