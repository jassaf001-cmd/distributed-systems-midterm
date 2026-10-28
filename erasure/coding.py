
import hashlib


class ErasureCoder:
    """
    XOR-based erasure coding with:
      k = 2 data fragments
      m = 1 parity fragment

    Can recover any single missing fragment.
    """

    K = 2
    M = 1

    @staticmethod
    def checksum(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def xor_bytes(a: bytes, b: bytes) -> bytes:
        if len(a) != len(b):
            raise ValueError("Fragment lengths must match")

        return bytes(x ^ y for x, y in zip(a, b))

    def encode(self, data: bytes):
        original_size = len(data)

        # Round up to accommodate odd-sized inputs.
        fragment_size = (original_size + 1) // 2

        padded = data.ljust(fragment_size * 2, b"\x00")

        fragment_a = padded[:fragment_size]
        fragment_b = padded[fragment_size:]

        parity = self.xor_bytes(fragment_a, fragment_b)

        fragments = [fragment_a, fragment_b, parity]

        return {
            "original_size": original_size,
            "original_hash": self.checksum(data),
            "fragments": [
                {
                    "index": index,
                    "data": fragment,
                    "hash": self.checksum(fragment),
                    "size": len(fragment),
                }
                for index, fragment in enumerate(fragments)
            ],
        }

    def decode(self, fragments: dict, original_size: int,
               original_hash: str = None):
        """
        fragments:
          {0: data_a, 1: data_b, 2: parity}

        Exactly one fragment may be missing.
        """

        if original_size < 0:
            raise ValueError("Invalid original size")

        if not set(fragments).issubset({0, 1, 2}):
            raise ValueError("Unknown fragment index")

        if len(fragments) < 2:
            raise ValueError(
                "At least two fragments are required"
            )

        available = dict(fragments)

        lengths = {len(value) for value in available.values()}

        if len(lengths) != 1:
            raise ValueError("Inconsistent fragment sizes")

        if next(iter(lengths)) != (original_size + 1) // 2:
            raise ValueError("Unexpected fragment length")

        if 0 not in available:
            available[0] = self.xor_bytes(
                available[1], available[2]
            )

        if 1 not in available:
            available[1] = self.xor_bytes(
                available[0], available[2]
            )

        if 2 in available:
            expected_parity = self.xor_bytes(
                available[0], available[1]
            )
            if expected_parity != available[2]:
                raise ValueError("Parity verification failed")

        restored = (
            available[0] + available[1]
        )[:original_size]

        if original_hash is not None:
            if self.checksum(restored) != original_hash:
                raise ValueError(
                    "Reconstructed data failed SHA-256"
                )

        return restored

    def repair(self, fragments: dict, missing_index: int):
        """Regenerate one missing fragment."""

        if missing_index not in (0, 1, 2):
            raise ValueError("Invalid fragment index")

        if missing_index in fragments:
            raise ValueError("Fragment is not missing")

        if set(fragments) != ({0, 1, 2} - {missing_index}):
            raise ValueError(
                "Exactly two surviving fragments required"
            )

        first, second = fragments.values()

        return self.xor_bytes(first, second)
