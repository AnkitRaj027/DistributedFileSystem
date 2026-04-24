"""
dfs_core.py — Core Distributed File System Engine
Handles: nodes, chunking, replication, heartbeat tracking, integrity checks
"""

import os
import uuid
import json
import time
import hashlib
import threading
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple
from enum import Enum

# ─────────────────────────────────────────────────────────────────────────────
# Enums & Constants
# ─────────────────────────────────────────────────────────────────────────────
#node statusS
class NodeStatus(str, Enum):
    ONLINE     = "online"
    OFFLINE    = "offline"
    RECOVERING = "recovering"

CHUNK_SIZE         = 512 * 1024   # 512 KB per chunk
HEARTBEAT_INTERVAL = 2            # seconds between heartbeats
HEARTBEAT_TIMEOUT  = 6            # seconds before node is declared dead
DATA_DIR           = "data"       # root storage directory


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChunkMeta:
    chunk_id:  str
    file_id:   str
    index:     int
    size:      int
    checksum:  str        # SHA-256 hex digest
    node_ids:  List[str]  # nodes that hold this chunk


@dataclass
class FileMeta:
    file_id:     str
    name:        str
    size:        int
    upload_time: float
    num_chunks:  int
    chunks:      List[ChunkMeta] = field(default_factory=list)
    checksum:    str = ""         # whole-file checksum


@dataclass
class NodeInfo:
    node_id:         str
    status:          NodeStatus
    storage_path:    str
    last_heartbeat:  float
    chunks_stored:   int  = 0
    bytes_stored:    int  = 0
    total_capacity:  int  = 5 * 1024 * 1024 * 1024  # 5 GB simulated


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def sha256_of(data: bytes) -> str:
    """Compute and return the SHA-256 hexadecimal checksum of the given bytes."""
    return hashlib.sha256(data).hexdigest()


def chunk_file(data: bytes) -> List[Tuple[int, bytes]]:
    """Split raw bytes into (index, chunk) tuples."""
    chunks = []
    for i in range(0, len(data), CHUNK_SIZE):
        chunks.append((i // CHUNK_SIZE, data[i: i + CHUNK_SIZE]))
    if not chunks:                 # empty file → one empty chunk
        chunks.append((0, b""))
    return chunks


# ─────────────────────────────────────────────────────────────────────────────
# Node Manager
# ─────────────────────────────────────────────────────────────────────────────

class NodeManager:
    """Manages the lifecycle of storage nodes."""

    def __init__(self):
        self._lock  = threading.RLock()
        self._nodes: Dict[str, NodeInfo] = {}

    # ── internal ──────────────────────────────────────────────────────────────

    def _node_dir(self, node_id: str) -> str:
        path = os.path.join(DATA_DIR, node_id)
        os.makedirs(path, exist_ok=True)
        return path

    # ── public ────────────────────────────────────────────────────────────────

    def add_node(self, node_id: Optional[str] = None) -> NodeInfo:
        with self._lock:
            if node_id is None:
                node_id = f"node_{len(self._nodes) + 1}"
            path = self._node_dir(node_id)
            info = NodeInfo(
                node_id        = node_id,
                status         = NodeStatus.ONLINE,
                storage_path   = path,
                last_heartbeat = time.time(),
            )
            self._nodes[node_id] = info
            return info

    def remove_node(self, node_id: str) -> bool:
        with self._lock:
            if node_id in self._nodes:
                del self._nodes[node_id]
                return True
            return False

    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        return self._nodes.get(node_id)

    def all_nodes(self) -> List[NodeInfo]:
        return list(self._nodes.values())

    def online_nodes(self) -> List[NodeInfo]:
        return [n for n in self._nodes.values() if n.status == NodeStatus.ONLINE]

    def heartbeat(self, node_id: str):
        with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].last_heartbeat = time.time()

    def fail_node(self, node_id: str):
        with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].status = NodeStatus.OFFLINE

    def recover_node(self, node_id: str):
        with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].status = NodeStatus.RECOVERING
                self._nodes[node_id].last_heartbeat = time.time()

    def check_heartbeats(self) -> List[str]:
        """Return list of node_ids that just timed out."""
        dead = []
        now  = time.time()
        with self._lock:
            for info in self._nodes.values():
                if info.status == NodeStatus.ONLINE:
                    if now - info.last_heartbeat > HEARTBEAT_TIMEOUT:
                        info.status = NodeStatus.OFFLINE
                        dead.append(info.node_id)
                elif info.status == NodeStatus.RECOVERING:
                    # After a brief recovery window, bring back online
                    if now - info.last_heartbeat > 2:
                        info.status = NodeStatus.ONLINE
        return dead

    def update_node_stats(self, node_id: str, delta_chunks: int, delta_bytes: int):
        with self._lock:
            n = self._nodes.get(node_id)
            if n:
                n.chunks_stored = max(0, n.chunks_stored + delta_chunks)
                n.bytes_stored  = max(0, n.bytes_stored  + delta_bytes)

    def to_dict(self) -> List[dict]:
        with self._lock:
            result = []
            for n in self._nodes.values():
                result.append({
                    "node_id":        n.node_id,
                    "status":         n.status.value,
                    "storage_path":   n.storage_path,
                    "last_heartbeat": n.last_heartbeat,
                    "chunks_stored":  n.chunks_stored,
                    "bytes_stored":   n.bytes_stored,
                    "total_capacity": n.total_capacity,
                    "used_pct":       round(n.bytes_stored / n.total_capacity * 100, 2),
                })
            return result


# ─────────────────────────────────────────────────────────────────────────────
# Chunk Store
# ─────────────────────────────────────────────────────────────────────────────

class ChunkStore:
    """Low-level disk read/write for chunks."""

    @staticmethod
    def _chunk_path(node: NodeInfo, chunk_id: str) -> str:
        return os.path.join(node.storage_path, chunk_id + ".chunk")

    @classmethod
    def write(cls, node: NodeInfo, chunk_id: str, data: bytes) -> bool:
        try:
            path = cls._chunk_path(node, chunk_id)
            with open(path, "wb") as f:
                f.write(data)
            return True
        except OSError:
            return False

    @classmethod
    def read(cls, node: NodeInfo, chunk_id: str) -> Optional[bytes]:
        try:
            path = cls._chunk_path(node, chunk_id)
            with open(path, "rb") as f:
                return f.read()
        except OSError:
            return None

    @classmethod
    def delete(cls, node: NodeInfo, chunk_id: str) -> bool:
        try:
            path = cls._chunk_path(node, chunk_id)
            if os.path.exists(path):
                os.remove(path)
            return True
        except OSError:
            return False

    @classmethod
    def exists(cls, node: NodeInfo, chunk_id: str) -> bool:
        return os.path.exists(cls._chunk_path(node, chunk_id))


# ─────────────────────────────────────────────────────────────────────────────
# Replication Manager
# ─────────────────────────────────────────────────────────────────────────────

class ReplicationManager:
    """Selects target nodes for chunk placement using round-robin + health."""

    def __init__(self, node_manager: NodeManager, replication_factor: int = 2):
        self.node_manager       = node_manager
        self.replication_factor = replication_factor
        self._rr_index          = 0
        self._lock              = threading.Lock()

    def select_nodes(self, count: int) -> List[NodeInfo]:
        """Pick `count` online nodes, round-robin style."""
        online = self.node_manager.online_nodes()
        if not online:
            return []
        selected = []
        with self._lock:
            for _ in range(min(count, len(online))):
                node = online[self._rr_index % len(online)]
                selected.append(node)
                self._rr_index += 1
        return selected

    def replicate_chunk(
        self, chunk_id: str, data: bytes, node_manager: NodeManager
    ) -> List[str]:
        """Write chunk to `replication_factor` nodes. Returns list of node_ids that succeeded."""
        targets    = self.select_nodes(self.replication_factor)
        successful = []
        for node in targets:
            ok = ChunkStore.write(node, chunk_id, data)
            if ok:
                node_manager.update_node_stats(node.node_id, 1, len(data))
                successful.append(node.node_id)
        return successful

    def ensure_replication(
        self,
        chunk_meta: ChunkMeta,
        chunk_data: bytes,
        node_manager: NodeManager,
    ) -> List[str]:
        """Re-replicate a chunk that is under-replicated. Returns new node list."""
        online  = {n.node_id for n in node_manager.online_nodes()}
        current = set(chunk_meta.node_ids) & online  # healthy replicas

        needed  = self.replication_factor - len(current)
        if needed <= 0:
            return list(current)

        candidates = [
            n for n in node_manager.online_nodes()
            if n.node_id not in current
        ][:needed]

        for node in candidates:
            ok = ChunkStore.write(node, chunk_meta.chunk_id, chunk_data)
            if ok:
                node_manager.update_node_stats(node.node_id, 1, chunk_meta.size)
                current.add(node.node_id)

        return list(current)


# ─────────────────────────────────────────────────────────────────────────────
# Integrity Checker
# ─────────────────────────────────────────────────────────────────────────────

class IntegrityChecker:
    """Validates chunk checksums against stored data."""

    @staticmethod
    def verify_chunk(node: NodeInfo, chunk_meta: ChunkMeta) -> Tuple[bool, str]:
        """Returns (is_valid, message)."""
        data = ChunkStore.read(node, chunk_meta.chunk_id)
        if data is None:
            return False, f"Chunk {chunk_meta.chunk_id} missing on {node.node_id}"
        actual = sha256_of(data)
        if actual != chunk_meta.checksum:
            return False, (
                f"Checksum mismatch on {node.node_id}: "
                f"expected {chunk_meta.checksum[:8]}…, got {actual[:8]}…"
            )
        return True, "OK"

    @staticmethod
    def verify_file(
        file_meta: FileMeta,
        node_manager: NodeManager,
    ) -> Dict[str, object]:
        """Full integrity scan for all chunks of a file."""
        results = {"file_id": file_meta.file_id, "chunks": []}
        for cm in file_meta.chunks:
            chunk_results = []
            for node_id in cm.node_ids:
                node = node_manager.get_node(node_id)
                if node is None or node.status != NodeStatus.ONLINE:
                    chunk_results.append({"node": node_id, "valid": False, "msg": "Node offline"})
                    continue
                ok, msg = IntegrityChecker.verify_chunk(node, cm)
                chunk_results.append({"node": node_id, "valid": ok, "msg": msg})
            results["chunks"].append({
                "chunk_id": cm.chunk_id,
                "index":    cm.index,
                "replicas": chunk_results,
            })
        return results


# ─────────────────────────────────────────────────────────────────────────────
# File Registry
# ─────────────────────────────────────────────────────────────────────────────

class FileRegistry:
    """In-memory registry of all files and their chunk metadata."""

    def __init__(self):
        self._lock  = threading.RLock()
        self._files: Dict[str, FileMeta] = {}

    def register(self, meta: FileMeta):
        with self._lock:
            self._files[meta.file_id] = meta

    def get(self, file_id: str) -> Optional[FileMeta]:
        return self._files.get(file_id)

    def all_files(self) -> List[FileMeta]:
        return list(self._files.values())

    def delete(self, file_id: str) -> Optional[FileMeta]:
        with self._lock:
            return self._files.pop(file_id, None)

    def update_chunk_nodes(self, file_id: str, chunk_id: str, node_ids: List[str]):
        with self._lock:
            fm = self._files.get(file_id)
            if fm:
                for cm in fm.chunks:
                    if cm.chunk_id == chunk_id:
                        cm.node_ids = node_ids
                        break

    def to_dict(self) -> List[dict]:
        with self._lock:
            result = []
            for fm in self._files.values():
                result.append({
                    "file_id":     fm.file_id,
                    "name":        fm.name,
                    "size":        fm.size,
                    "upload_time": fm.upload_time,
                    "num_chunks":  fm.num_chunks,
                    "checksum":    fm.checksum,
                    "chunks": [
                        {
                            "chunk_id": cm.chunk_id,
                            "index":    cm.index,
                            "size":     cm.size,
                            "checksum": cm.checksum,
                            "node_ids": cm.node_ids,
                        }
                        for cm in fm.chunks
                    ],
                })
            return result


# ─────────────────────────────────────────────────────────────────────────────
# Distributed File System (facade)
# ─────────────────────────────────────────────────────────────────────────────

class DistributedFileSystem:
    """
    High-level facade that orchestrates all DFS operations.
    Thread-safe. Designed to be used by the Flask server.
    """

    def __init__(self, replication_factor: int = 2, initial_nodes: int = 3):
        self.node_manager        = NodeManager()
        self.replication_manager = ReplicationManager(self.node_manager, replication_factor)
        self.file_registry       = FileRegistry()
        self._event_log: List[dict] = []
        self._log_lock   = threading.Lock()
        self.replication_factor  = replication_factor

        # Bootstrap nodes
        for i in range(1, initial_nodes + 1):
            self.node_manager.add_node(f"node_{i}")

        # Start heartbeat background thread
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb_thread.start()

    # ── event log ─────────────────────────────────────────────────────────────

    def _log(self, event_type: str, message: str, **kwargs):
        entry = {
            "ts":      time.time(),
            "type":    event_type,
            "message": message,
            **kwargs,
        }
        with self._log_lock:
            self._event_log.append(entry)
            if len(self._event_log) > 200:
                self._event_log = self._event_log[-200:]

    def get_events(self, since: float = 0) -> List[dict]:
        with self._log_lock:
            return [e for e in self._event_log if e["ts"] > since]

    # ── heartbeat loop ────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            # Simulate nodes sending heartbeats (all online nodes "check in")
            for node in self.node_manager.online_nodes():
                self.node_manager.heartbeat(node.node_id)

            dead = self.node_manager.check_heartbeats()
            for node_id in dead:
                self._log("node_failure", f"Node {node_id} declared DEAD (heartbeat timeout)", node_id=node_id)
                self._re_replicate_for_dead_node(node_id)

    def _re_replicate_for_dead_node(self, dead_node_id: str):
        """Re-replicate all chunks that were on the dead node."""
        for fm in self.file_registry.all_files():
            for cm in fm.chunks:
                if dead_node_id not in cm.node_ids:
                    continue
                # Try to find a healthy replica to read from
                data = self._read_chunk_from_any_replica(cm)
                if data is None:
                    self._log("replication_fail", f"Cannot re-replicate chunk {cm.chunk_id}: no healthy replica", chunk_id=cm.chunk_id)
                    continue
                new_nodes = self.replication_manager.ensure_replication(cm, data, self.node_manager)
                self.file_registry.update_chunk_nodes(fm.file_id, cm.chunk_id, new_nodes)
                self._log("replication", f"Re-replicated chunk {cm.chunk_id[:8]}… to {new_nodes}", chunk_id=cm.chunk_id)

    def _read_chunk_from_any_replica(self, cm: ChunkMeta) -> Optional[bytes]:
        for node_id in cm.node_ids:
            node = self.node_manager.get_node(node_id)
            if node and node.status == NodeStatus.ONLINE:
                data = ChunkStore.read(node, cm.chunk_id)
                if data is not None:
                    return data
        return None

    # ── file operations ───────────────────────────────────────────────────────

    def upload_file(self, filename: str, data: bytes) -> FileMeta:
        file_id    = str(uuid.uuid4())
        raw_chunks = chunk_file(data)
        file_meta  = FileMeta(
            file_id     = file_id,
            name        = filename,
            size        = len(data),
            upload_time = time.time(),
            num_chunks  = len(raw_chunks),
            checksum    = sha256_of(data),
        )

        for idx, chunk_data in raw_chunks:
            chunk_id   = str(uuid.uuid4())
            checksum   = sha256_of(chunk_data)
            placed_on  = self.replication_manager.replicate_chunk(chunk_id, chunk_data, self.node_manager)

            cm = ChunkMeta(
                chunk_id = chunk_id,
                file_id  = file_id,
                index    = idx,
                size     = len(chunk_data),
                checksum = checksum,
                node_ids = placed_on,
            )
            file_meta.chunks.append(cm)

        self.file_registry.register(file_meta)
        self._log("upload", f"Uploaded '{filename}' ({len(data)} bytes, {len(raw_chunks)} chunks)", file_id=file_id)
        return file_meta

    def download_file(self, file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
        """Returns (assembled_data, filename) or (None, error_msg)."""
        fm = self.file_registry.get(file_id)
        if fm is None:
            return None, "File not found"

        assembled = bytearray()
        for cm in sorted(fm.chunks, key=lambda c: c.index):
            data = self._read_chunk_from_any_replica(cm)
            if data is None:
                return None, f"Chunk {cm.index} unavailable on all replicas"
            # Integrity check
            if sha256_of(data) != cm.checksum:
                return None, f"Chunk {cm.index} failed integrity check"
            assembled.extend(data)

        self._log("download", f"Downloaded '{fm.name}'", file_id=file_id)
        return bytes(assembled), fm.name

    def delete_file(self, file_id: str) -> bool:
        fm = self.file_registry.delete(file_id)
        if fm is None:
            return False
        for cm in fm.chunks:
            for node_id in cm.node_ids:
                node = self.node_manager.get_node(node_id)
                if node:
                    ChunkStore.delete(node, cm.chunk_id)
                    self.node_manager.update_node_stats(node_id, -1, -cm.size)
        self._log("delete", f"Deleted file '{fm.name}'", file_id=file_id)
        return True

    # ── node operations ───────────────────────────────────────────────────────

    def add_node(self) -> NodeInfo:
        existing = len(self.node_manager.all_nodes())
        node_id  = f"node_{existing + 1}"
        info     = self.node_manager.add_node(node_id)
        self._log("node_add", f"Node {node_id} added to cluster", node_id=node_id)
        return info

    def fail_node(self, node_id: str) -> bool:
        node = self.node_manager.get_node(node_id)
        if not node:
            return False
        self.node_manager.fail_node(node_id)
        self._log("node_failure", f"Node {node_id} manually failed", node_id=node_id)
        self._re_replicate_for_dead_node(node_id)
        return True

    def recover_node(self, node_id: str) -> bool:
        node = self.node_manager.get_node(node_id)
        if not node:
            return False
        self.node_manager.recover_node(node_id)
        self._log("node_recovery", f"Node {node_id} recovering", node_id=node_id)
        return True

    def delete_node(self, node_id: str) -> tuple:
        """
        Permanently remove a node from the cluster.
        Must be OFFLINE first. Re-replicates any chunks that were on it
        to remaining healthy nodes before evicting.
        Returns (success: bool, message: str).
        """
        node = self.node_manager.get_node(node_id)
        if not node:
            return False, "Node not found"
        if node.status == NodeStatus.ONLINE:
            return False, "Node is still online — fail it first"

        online_count = len(self.node_manager.online_nodes())
        if online_count < 1:
            return False, "No online nodes left to absorb the data"

        # Re-replicate every chunk that lived on this node
        re_rep_count = 0
        for fm in self.file_registry.all_files():
            for cm in fm.chunks:
                if node_id not in cm.node_ids:
                    continue
                # Remove the dead node from the replica list before re-replicating
                cm.node_ids = [nid for nid in cm.node_ids if nid != node_id]
                data = self._read_chunk_from_any_replica(cm)
                if data is not None:
                    new_nodes = self.replication_manager.ensure_replication(
                        cm, data, self.node_manager
                    )
                    self.file_registry.update_chunk_nodes(fm.file_id, cm.chunk_id, new_nodes)
                    re_rep_count += 1

        # Evict node from registry
        self.node_manager.remove_node(node_id)
        msg = f"Node {node_id} removed. Re-replicated {re_rep_count} chunk(s)."
        self._log("node_delete", msg, node_id=node_id)
        return True, msg

    # ── system stats ──────────────────────────────────────────────────────────

    def system_stats(self) -> dict:
        all_nodes    = self.node_manager.all_nodes()
        online_nodes = [n for n in all_nodes if n.status == NodeStatus.ONLINE]
        all_files    = self.file_registry.all_files()

        # Files at risk: any chunk with fewer online replicas than replication_factor
        at_risk = 0
        for fm in all_files:
            for cm in fm.chunks:
                online_replicas = sum(
                    1 for nid in cm.node_ids
                    if (n := self.node_manager.get_node(nid)) and n.status == NodeStatus.ONLINE
                )
                if online_replicas < self.replication_factor:
                    at_risk += 1
                    break

        total_bytes  = sum(n.bytes_stored for n in all_nodes)
        availability = (len(online_nodes) / len(all_nodes) * 100) if all_nodes else 0

        return {
            "total_nodes":        len(all_nodes),
            "online_nodes":       len(online_nodes),
            "offline_nodes":      len(all_nodes) - len(online_nodes),
            "total_files":        len(all_files),
            "files_at_risk":      at_risk,
            "total_bytes_stored": total_bytes,
            "replication_factor": self.replication_factor,
            "availability_pct":   round(availability, 1),
        }

    def integrity_scan(self, file_id: str) -> dict:
        fm = self.file_registry.get(file_id)
        if not fm:
            return {"error": "File not found"}
        return IntegrityChecker.verify_file(fm, self.node_manager)
