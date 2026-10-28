
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
