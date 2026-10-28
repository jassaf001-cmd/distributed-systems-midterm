
import bisect
import hashlib


class ConsistentHashRing:
    def __init__(self, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = []
        self.node_map = {}
        self.nodes = set()

    @staticmethod
    def hash_value(value):
        digest = hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

        return int(digest, 16)

    def add_node(self, node_id):
        if node_id in self.nodes:
            return

        self.nodes.add(node_id)

        for index in range(self.virtual_nodes):
            virtual_key = f"{node_id}:{index}"
            position = self.hash_value(virtual_key)

            bisect.insort(self.ring, position)
            self.node_map[position] = node_id

    def remove_node(self, node_id):
        if node_id not in self.nodes:
            return

        self.nodes.remove(node_id)

        for index in range(self.virtual_nodes):
            virtual_key = f"{node_id}:{index}"
            position = self.hash_value(virtual_key)

            self.ring.remove(position)
            del self.node_map[position]

    def get_nodes(self, chunk_hash, replica_count=3):
        if not self.ring:
            raise ValueError("No storage nodes available")

        if replica_count < 1:
            raise ValueError("Replica count must be positive")

        if replica_count > len(self.nodes):
            raise ValueError(
                "Not enough distinct nodes for requested replicas"
            )

        position = self.hash_value(chunk_hash)

        index = bisect.bisect_left(
            self.ring, position
        )

        selected = []
        seen = set()

        for offset in range(len(self.ring)):
            ring_index = (
                index + offset
            ) % len(self.ring)

            node_id = self.node_map[
                self.ring[ring_index]
            ]

            if node_id not in seen:
                selected.append(node_id)
                seen.add(node_id)

            if len(selected) == replica_count:
                break

        return selected


NODE_URLS = {
    "node1": "http://127.0.0.1:8001",
    "node2": "http://127.0.0.1:8002",
    "node3": "http://127.0.0.1:8003",
    "node4": "http://127.0.0.1:8004"
}


def create_ring():
    ring = ConsistentHashRing(virtual_nodes=100)

    for node_id in NODE_URLS:
        ring.add_node(node_id)

    return ring
