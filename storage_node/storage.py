
import hashlib
import os
import shutil
import tempfile
from pathlib import Path


class ChunkStorage:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def chunk_path(self, chunk_hash: str) -> Path:
        return self.data_dir / chunk_hash

    async def save_chunk(self, chunk_hash: str, stream):
        hasher = hashlib.sha256()
        total_size = 0
        temp_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.data_dir,
                delete=False
            ) as temp_file:
                temp_path = Path(temp_file.name)

                async for data in stream:
                    if not data:
                        continue

                    hasher.update(data)
                    temp_file.write(data)
                    total_size += len(data)

            calculated_hash = hasher.hexdigest()

            if calculated_hash != chunk_hash:
                raise ValueError(
                    "SHA-256 mismatch: chunk contents do not match its ID"
                )

            final_path = self.chunk_path(chunk_hash)
            os.replace(temp_path, final_path)

            return {
                "chunk_hash": chunk_hash,
                "size": total_size,
                "status": "stored"
            }

        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def verify_chunk(self, chunk_hash: str) -> bool:
        path = self.chunk_path(chunk_hash)

        if not path.is_file():
            return False

        hasher = hashlib.sha256()

        with path.open("rb") as file:
            for data in iter(lambda: file.read(1024 * 1024), b""):
                hasher.update(data)

        return hasher.hexdigest() == chunk_hash

    def delete_chunk(self, chunk_hash: str) -> bool:
        path = self.chunk_path(chunk_hash)

        if not path.is_file():
            return False

        path.unlink()
        return True

    def get_stats(self):
        chunks = [
            path for path in self.data_dir.iterdir()
            if path.is_file()
            and len(path.name) == 64
            and all(c in "0123456789abcdef" for c in path.name)
        ]

        disk = shutil.disk_usage(self.data_dir)

        return {
            "chunk_count": len(chunks),
            "stored_bytes": sum(p.stat().st_size for p in chunks),
            "free_disk_bytes": disk.free,
            "total_disk_bytes": disk.total
        }
