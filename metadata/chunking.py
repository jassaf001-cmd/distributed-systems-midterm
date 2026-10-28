
import hashlib


CHUNK_SIZE = 4 * 1024 * 1024


def split_file(file_path, chunk_size=CHUNK_SIZE):
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")

    with open(file_path, "rb") as file:
        position = 0

        while True:
            data = file.read(chunk_size)

            if not data:
                break

            chunk_hash = hashlib.sha256(
                data
            ).hexdigest()

            yield {
                "position": position,
                "hash": chunk_hash,
                "size": len(data),
                "data": data
            }

            position += 1

import random

_rng = random.Random(42)
GEAR = [_rng.getrandbits(64) for _ in range(256)]


def content_defined_chunks(
    file_path,
    min_size=256 * 1024,
    avg_size=1024 * 1024,
    max_size=4 * 1024 * 1024
):
    if not (
        0 < min_size <= avg_size <= max_size
        and avg_size & (avg_size - 1) == 0
    ):
        raise ValueError("Invalid chunk size settings")

    mask = avg_size - 1
    buffer = bytearray()
    rolling_hash = 0
    position = 0

    with open(file_path, "rb") as source:
        while True:
            block = source.read(64 * 1024)

            if not block:
                break

            for byte in block:
                buffer.append(byte)

                rolling_hash = (
                    (rolling_hash << 1) + GEAR[byte]
                ) & 0xFFFFFFFFFFFFFFFF

                boundary = (
                    len(buffer) >= min_size
                    and (rolling_hash & mask) == 0
                )

                if boundary or len(buffer) >= max_size:
                    data = bytes(buffer)

                    yield {
                        "position": position,
                        "hash": hashlib.sha256(data).hexdigest(),
                        "size": len(data),
                        "data": data
                    }

                    position += 1
                    buffer.clear()
                    rolling_hash = 0

    if buffer:
        data = bytes(buffer)

        yield {
            "position": position,
            "hash": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "data": data
        }
