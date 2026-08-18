"""
dfs_core.py — Core Distributed File System Engine (v2 — Next Level)
Features: zlib compression, AES-256 encryption at rest, concurrent replication
"""

import os
import uuid
import json
import time
import zlib
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

# Optional AES encryption via `cryptography` package
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    import secrets
    ENCRYPTION_ENABLED = True
except ImportError:
    ENCRYPTION_ENABLED = False

# ─────────────────────────────────────────────────────────────────────────────
# Enums & Constants
# ─────────────────────────────────────────────────────────────────────────────

class NodeStatus(str, Enum):
    ONLINE     = "online"
    OFFLINE    = "offline"
    RECOVERING = "recovering"

CHUNK_SIZE         = 512 * 1024   # 512 KB per chunk
HEARTBEAT_INTERVAL = 2            # seconds between heartbeats
HEARTBEAT_TIMEOUT  = 6            # seconds before node is declared dead
if os.environ.get("VERCEL"):
    DATA_DIR = "/tmp/data"
else:
    DATA_DIR = "data"
MAX_WORKERS        = 8            # thread pool size for parallel ops

# Derive a stable 32-byte AES key from a master secret (in production, load from env/vault)
_MASTER_SECRET = b"dfs-master-secret-key-v2-2026!!"  # 32 bytes
_AES_KEY = hashlib.sha256(_MASTER_SECRET).digest()


# ─────────────────────────────────────────────────────────────────────────────
# Encryption helpers
# ─────────────────────────────────────────────────────────────────────────────

def encrypt_chunk(data: bytes) -> bytes:
    """AES-256-GCM encrypt. Returns nonce(12) + ciphertext."""
    if not ENCRYPTION_ENABLED:
        return data
    aesgcm = AESGCM(_AES_KEY)
    nonce = secrets.token_bytes(12)
    ct = aesgcm.encrypt(nonce, data, None)
    return nonce + ct


def decrypt_chunk(data: bytes) -> bytes:
    """AES-256-GCM decrypt. Expects nonce(12) + ciphertext."""
    if not ENCRYPTION_ENABLED:
        return data
    aesgcm = AESGCM(_AES_KEY)
    nonce, ct = data[:12], data[12:]
    return aesgcm.decrypt(nonce, ct, None)


# ─────────────────────────────────────────────────────────────────────────────
# Compression helpers
# ─────────────────────────────────────────────────────────────────────────────

def compress_chunk(data: bytes) -> bytes:
    """zlib compress at level 6 for a good speed/ratio balance."""
    return zlib.compress(data, level=6)


def decompress_chunk(data: bytes) -> bytes:
    return zlib.decompress(data)


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChunkMeta:
    chunk_id:       str
    file_id:        str
    index:          int
    size:           int           # original (pre-compression) size
    stored_size:    int           # size on disk (post-compress/encrypt)
    checksum:       str           # SHA-256 of original data
    node_ids:       List[str]
    compressed:     bool = True
    encrypted:      bool = True


@dataclass
class FileMeta:
    file_id:     str
    name:        str
    size:        int
    upload_time: float
    num_chunks:  int
    chunks:      List[ChunkMeta] = field(default_factory=list)
    checksum:    str = ""


@dataclass
class NodeInfo:
    node_id:        str
    status:         NodeStatus
    storage_path:   str
    last_heartbeat: float
    chunks_stored:  int = 0
    bytes_stored:   int = 0
    total_capacity: int = 5 * 1024 * 1024 * 1024  # 5 GB simulated


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def chunk_file(data: bytes) -> List[Tuple[int, bytes]]:
    """Split raw bytes into (index, chunk) tuples."""
    chunks = []
    for i in range(0, len(data), CHUNK_SIZE):
        chunks.append((i // CHUNK_SIZE, data[i: i + CHUNK_SIZE]))
    if not chunks:
        chunks.append((0, b""))
    return chunks


def prepare_chunk(raw: bytes) -> bytes:
    """Compress then encrypt a raw chunk for storage."""
    return encrypt_chunk(compress_chunk(raw))


def recover_chunk(stored: bytes) -> bytes:
    """Decrypt then decompress a stored chunk."""
    return decompress_chunk(decrypt_chunk(stored))


# ─────────────────────────────────────────────────────────────────────────────
# Node Manager
# ─────────────────────────────────────────────────────────────────────────────

class NodeManager:
    """Manages the lifecycle of storage nodes."""

    def __init__(self):
        self._lock  = threading.RLock()
        self._nodes: Dict[str, NodeInfo] = {}

    def _node_dir(self, node_id: str) -> str:
        path = os.path.join(DATA_DIR, node_id)
        os.makedirs(path, exist_ok=True)
        return path

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
        dead = []
        now  = time.time()
        with self._lock:
            for info in self._nodes.values():
                if info.status == NodeStatus.ONLINE:
                    if now - info.last_heartbeat > HEARTBEAT_TIMEOUT:
                        info.status = NodeStatus.OFFLINE
                        dead.append(info.node_id)
                elif info.status == NodeStatus.RECOVERING:
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
    """Low-level disk read/write for chunks (stored compressed+encrypted)."""

    @staticmethod
    def _chunk_path(node: NodeInfo, chunk_id: str) -> str:
        return os.path.join(node.storage_path, chunk_id + ".chunk")

    @classmethod
    def write(cls, node: NodeInfo, chunk_id: str, raw_data: bytes) -> Tuple[bool, int]:
        """Compress + encrypt then write. Returns (success, stored_size)."""
        try:
            payload = prepare_chunk(raw_data)
            path = cls._chunk_path(node, chunk_id)
            with open(path, "wb") as f:
                f.write(payload)
            return True, len(payload)
        except Exception:
            return False, 0

    @classmethod
    def read(cls, node: NodeInfo, chunk_id: str) -> Optional[bytes]:
        """Read, decrypt, decompress. Returns original bytes or None."""
        try:
            path = cls._chunk_path(node, chunk_id)
            with open(path, "rb") as f:
                payload = f.read()
            return recover_chunk(payload)
        except Exception:
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
# Replication Manager  (concurrent)
# ─────────────────────────────────────────────────────────────────────────────

class ReplicationManager:
    """Selects target nodes using round-robin + replicates chunks concurrently."""

    def __init__(self, node_manager: NodeManager, replication_factor: int = 2):
        self.node_manager       = node_manager
        self.replication_factor = replication_factor
        self._rr_index          = 0
        self._lock              = threading.Lock()
        self._executor          = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="dfs-rep")

    def select_nodes(self, count: int) -> List[NodeInfo]:
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

    def _write_to_node(self, node: NodeInfo, chunk_id: str, data: bytes):
        ok, stored_size = ChunkStore.write(node, chunk_id, data)
        if ok:
            self.node_manager.update_node_stats(node.node_id, 1, stored_size)
            return node.node_id
        return None

    def replicate_chunk(self, chunk_id: str, data: bytes, node_manager: NodeManager) -> Tuple[List[str], int]:
        """Write chunk to replication_factor nodes concurrently.
        Returns (list of node_ids that succeeded, stored_size)."""
        targets = self.select_nodes(self.replication_factor)
        successful = []
        stored_size = 0
        futures = {
            self._executor.submit(self._write_to_node, node, chunk_id, data): node
            for node in targets
        }
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                successful.append(result)
                stored_size = len(prepare_chunk(data))  # same for all nodes
        return successful, stored_size

    def ensure_replication(
        self,
        chunk_meta: ChunkMeta,
        chunk_data: bytes,
        node_manager: NodeManager,
    ) -> List[str]:
        online  = {n.node_id for n in node_manager.online_nodes()}
        current = set(chunk_meta.node_ids) & online
        needed  = self.replication_factor - len(current)
        if needed <= 0:
            return list(current)

        candidates = [
            n for n in node_manager.online_nodes()
            if n.node_id not in current
        ][:needed]

        futures = {
            self._executor.submit(self._write_to_node, node, chunk_meta.chunk_id, chunk_data): node
            for node in candidates
        }
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                current.add(result)

        return list(current)


# ─────────────────────────────────────────────────────────────────────────────
# Integrity Checker
# ─────────────────────────────────────────────────────────────────────────────

class IntegrityChecker:

    @staticmethod
    def verify_chunk(node: NodeInfo, chunk_meta: ChunkMeta) -> Tuple[bool, str]:
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
    def verify_file(file_meta: FileMeta, node_manager: NodeManager) -> Dict[str, object]:
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
                            "chunk_id":    cm.chunk_id,
                            "index":       cm.index,
                            "size":        cm.size,
                            "stored_size": cm.stored_size,
                            "checksum":    cm.checksum,
                            "node_ids":    cm.node_ids,
                            "compressed":  cm.compressed,
                            "encrypted":   cm.encrypted,
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
    Thread-safe. Features: compression, AES-256 encryption, concurrent replication.
    """

    def __init__(self, replication_factor: int = 2, initial_nodes: int = 3):
        self.node_manager        = NodeManager()
        self.replication_manager = ReplicationManager(self.node_manager, replication_factor)
        self.file_registry       = FileRegistry()
        self._event_log: List[dict] = []
        self._log_lock   = threading.Lock()
        self.replication_factor  = replication_factor
        self.encryption_enabled  = ENCRYPTION_ENABLED

        # Try to load existing state
        state_loaded = self.load_state()

        if not state_loaded:
            # Bootstrap default nodes
            for i in range(1, initial_nodes + 1):
                self.node_manager.add_node(f"node_{i}")
            self._log("system", "DFS cluster initialized with default settings")
            self.save_state()

        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb_thread.start()

    def save_state(self):
        """Serialize current state (nodes, files, events) to disk for serverless persistence."""
        state_path = os.path.join(DATA_DIR, "dfs_metadata.json")
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            nodes_data = []
            for n in self.node_manager.all_nodes():
                nodes_data.append({
                    "node_id": n.node_id,
                    "status": n.status.value,
                    "storage_path": n.storage_path,
                    "last_heartbeat": n.last_heartbeat,
                    "chunks_stored": n.chunks_stored,
                    "bytes_stored": n.bytes_stored,
                    "total_capacity": n.total_capacity,
                })
            
            files_data = []
            for fm in self.file_registry.all_files():
                files_data.append({
                    "file_id": fm.file_id,
                    "name": fm.name,
                    "size": fm.size,
                    "upload_time": fm.upload_time,
                    "num_chunks": fm.num_chunks,
                    "checksum": fm.checksum,
                    "chunks": [
                        {
                            "chunk_id": cm.chunk_id,
                            "file_id": cm.file_id,
                            "index": cm.index,
                            "size": cm.size,
                            "stored_size": cm.stored_size,
                            "checksum": cm.checksum,
                            "node_ids": cm.node_ids,
                            "compressed": cm.compressed,
                            "encrypted": cm.encrypted,
                        } for cm in fm.chunks
                    ]
                })

            with self._log_lock:
                events_data = list(self._event_log)

            state = {
                "replication_factor": self.replication_factor,
                "nodes": nodes_data,
                "files": files_data,
                "events": events_data,
            }
            with open(state_path, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"[DFS] Failed to save state: {e}")

    def load_state(self) -> bool:
        """Deserialize state from disk if available."""
        state_path = os.path.join(DATA_DIR, "dfs_metadata.json")
        if not os.path.exists(state_path):
            return False
        try:
            with open(state_path, "r") as f:
                state = json.load(f)
            
            self.replication_factor = state.get("replication_factor", self.replication_factor)
            self.replication_manager.replication_factor = self.replication_factor
            
            nodes_dict = {}
            for n_data in state.get("nodes", []):
                os.makedirs(n_data["storage_path"], exist_ok=True)
                info = NodeInfo(
                    node_id = n_data["node_id"],
                    status = NodeStatus(n_data["status"]),
                    storage_path = n_data["storage_path"],
                    last_heartbeat = n_data["last_heartbeat"],
                    chunks_stored = n_data.get("chunks_stored", 0),
                    bytes_stored = n_data.get("bytes_stored", 0),
                    total_capacity = n_data.get("total_capacity", 5 * 1024 * 1024 * 1024),
                )
                nodes_dict[info.node_id] = info
            
            with self.node_manager._lock:
                self.node_manager._nodes = nodes_dict

            files_dict = {}
            for f_data in state.get("files", []):
                fm = FileMeta(
                    file_id = f_data["file_id"],
                    name = f_data["name"],
                    size = f_data["size"],
                    upload_time = f_data["upload_time"],
                    num_chunks = f_data["num_chunks"],
                    checksum = f_data["checksum"],
                )
                chunks = []
                for c_data in f_data.get("chunks", []):
                    chunks.append(ChunkMeta(
                        chunk_id = c_data["chunk_id"],
                        file_id = c_data["file_id"],
                        index = c_data["index"],
                        size = c_data["size"],
                        stored_size = c_data["stored_size"],
                        checksum = c_data["checksum"],
                        node_ids = c_data["node_ids"],
                        compressed = c_data.get("compressed", True),
                        encrypted = c_data.get("encrypted", True),
                    ))
                fm.chunks = chunks
                files_dict[fm.file_id] = fm
            
            with self.file_registry._lock:
                self.file_registry._files = files_dict

            with self._log_lock:
                self._event_log = state.get("events", [])
            
            print(f"[DFS] State loaded successfully from {state_path}")
            return True
        except Exception as e:
            print(f"[DFS] Failed to load state: {e}")
            return False

    def tick(self):
        """Simulate single clock cycle checks for recovering nodes in serverless env."""
        now = time.time()
        changed = False
        with self.node_manager._lock:
            for node in self.node_manager.all_nodes():
                if node.status == NodeStatus.RECOVERING:
                    if now - node.last_heartbeat > 2:
                        node.status = NodeStatus.ONLINE
                        self._log("node_online", f"Node {node.node_id} is now ONLINE after recovery", node_id=node.node_id)
                        changed = True
        if changed:
            self.save_state()

    # ── event log ─────────────────────────────────────────────────────────────

    def _log(self, event_type: str, message: str, **kwargs):
        entry = {"ts": time.time(), "type": event_type, "message": message, **kwargs}
        with self._log_lock:
            self._event_log.append(entry)
            if len(self._event_log) > 300:
                self._event_log = self._event_log[-300:]

    def get_events(self, since: float = 0) -> List[dict]:
        with self._log_lock:
            return [e for e in self._event_log if e["ts"] > since]

    # ── heartbeat loop ────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            for node in self.node_manager.online_nodes():
                self.node_manager.heartbeat(node.node_id)
            dead = self.node_manager.check_heartbeats()
            for node_id in dead:
                self._log("node_failure", f"Node {node_id} declared DEAD (heartbeat timeout)", node_id=node_id)
                self._re_replicate_for_dead_node(node_id)

    def _re_replicate_for_dead_node(self, dead_node_id: str):
        replicated_any = False
        for fm in self.file_registry.all_files():
            for cm in fm.chunks:
                if dead_node_id not in cm.node_ids:
                    continue
                data = self._read_chunk_from_any_replica(cm)
                if data is None:
                    self._log("replication_fail", f"Cannot re-replicate chunk {cm.chunk_id}: no healthy replica", chunk_id=cm.chunk_id)
                    continue
                new_nodes = self.replication_manager.ensure_replication(cm, data, self.node_manager)
                self.file_registry.update_chunk_nodes(fm.file_id, cm.chunk_id, new_nodes)
                self._log("replication", f"Re-replicated chunk {cm.chunk_id[:8]}… to {new_nodes}", chunk_id=cm.chunk_id)
                replicated_any = True
        if replicated_any:
            self.save_state()

    def _read_chunk_from_any_replica(self, cm: ChunkMeta) -> Optional[bytes]:
        for node_id in cm.node_ids:
            node = self.node_manager.get_node(node_id)
            if node and node.status == NodeStatus.ONLINE:
                data = ChunkStore.read(node, cm.chunk_id)
                if data is not None:
                    return data
        return None

    # ── concurrent upload ─────────────────────────────────────────────────────

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

        def process_chunk(idx_chunk):
            idx, chunk_data = idx_chunk
            chunk_id   = str(uuid.uuid4())
            checksum   = sha256_of(chunk_data)
            placed_on, stored_size = self.replication_manager.replicate_chunk(
                chunk_id, chunk_data, self.node_manager
            )
            return ChunkMeta(
                chunk_id    = chunk_id,
                file_id     = file_id,
                index       = idx,
                size        = len(chunk_data),
                stored_size = stored_size,
                checksum    = checksum,
                node_ids    = placed_on,
                compressed  = True,
                encrypted   = ENCRYPTION_ENABLED,
            )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="dfs-upload") as upload_executor:
            futures  = [upload_executor.submit(process_chunk, ic) for ic in raw_chunks]
            chunk_metas = [f.result() for f in as_completed(futures)]
        chunk_metas.sort(key=lambda c: c.index)
        file_meta.chunks = chunk_metas

        self.file_registry.register(file_meta)
        compression_note = " | compressed+encrypted" if ENCRYPTION_ENABLED else " | compressed"
        self._log("upload", f"Uploaded '{filename}' ({len(data)} bytes, {len(raw_chunks)} chunks{compression_note})", file_id=file_id)
        self.save_state()
        return file_meta

    # ── download ──────────────────────────────────────────────────────────────

    def download_file(self, file_id: str) -> Tuple[Optional[bytes], Optional[str]]:
        fm = self.file_registry.get(file_id)
        if fm is None:
            return None, "File not found"

        assembled = bytearray()
        for cm in sorted(fm.chunks, key=lambda c: c.index):
            data = self._read_chunk_from_any_replica(cm)
            if data is None:
                return None, f"Chunk {cm.index} unavailable on all replicas"
            if sha256_of(data) != cm.checksum:
                return None, f"Chunk {cm.index} failed integrity check"
            assembled.extend(data)

        self._log("download", f"Downloaded '{fm.name}'", file_id=file_id)
        return bytes(assembled), fm.name

    # ── delete ────────────────────────────────────────────────────────────────

    def delete_file(self, file_id: str) -> bool:
        fm = self.file_registry.delete(file_id)
        if fm is None:
            return False
        for cm in fm.chunks:
            for node_id in cm.node_ids:
                node = self.node_manager.get_node(node_id)
                if node:
                    ChunkStore.delete(node, cm.chunk_id)
                    self.node_manager.update_node_stats(node_id, -1, -cm.stored_size)
        self._log("delete", f"Deleted file '{fm.name}'", file_id=file_id)
        self.save_state()
        return True

    # ── node operations ───────────────────────────────────────────────────────

    def add_node(self) -> NodeInfo:
        existing = len(self.node_manager.all_nodes())
        node_id  = f"node_{existing + 1}"
        info     = self.node_manager.add_node(node_id)
        self._log("node_add", f"Node {node_id} added to cluster", node_id=node_id)
        self.save_state()
        return info

    def fail_node(self, node_id: str) -> bool:
        node = self.node_manager.get_node(node_id)
        if not node:
            return False
        self.node_manager.fail_node(node_id)
        self._log("node_failure", f"Node {node_id} manually failed", node_id=node_id)
        self._re_replicate_for_dead_node(node_id)
        self.save_state()
        return True

    def recover_node(self, node_id: str) -> bool:
        node = self.node_manager.get_node(node_id)
        if not node:
            return False
        self.node_manager.recover_node(node_id)
        self._log("node_recovery", f"Node {node_id} recovering", node_id=node_id)
        self.save_state()
        return True

    def delete_node(self, node_id: str) -> Tuple[bool, str]:
        node = self.node_manager.get_node(node_id)
        if not node:
            return False, "Node not found"
        if node.status == NodeStatus.ONLINE:
            return False, "Node is still online — fail it first"

        online_count = len(self.node_manager.online_nodes())
        if online_count < 1:
            return False, "No online nodes left to absorb the data"

        re_rep_count = 0
        for fm in self.file_registry.all_files():
            for cm in fm.chunks:
                if node_id not in cm.node_ids:
                    continue
                
                data = self._read_chunk_from_any_replica(cm)
                if data is not None:
                    new_nodes = self.replication_manager.ensure_replication(cm, data, self.node_manager)
                    self.file_registry.update_chunk_nodes(fm.file_id, cm.chunk_id, new_nodes)
                    re_rep_count += 1
                else:
                    new_node_ids = [nid for nid in cm.node_ids if nid != node_id]
                    self.file_registry.update_chunk_nodes(fm.file_id, cm.chunk_id, new_node_ids)

        self.node_manager.remove_node(node_id)
        msg = f"Node {node_id} removed. Re-replicated {re_rep_count} chunk(s)."
        self._log("node_delete", msg, node_id=node_id)
        self.save_state()
        return True, msg

    # ── system stats ──────────────────────────────────────────────────────────

    def system_stats(self) -> dict:
        all_nodes    = self.node_manager.all_nodes()
        online_nodes = [n for n in all_nodes if n.status == NodeStatus.ONLINE]
        all_files    = self.file_registry.all_files()

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
            "encryption_enabled": ENCRYPTION_ENABLED,
            "compression_enabled": True,
        }

    def integrity_scan(self, file_id: str) -> Dict[str, object]:
        fm = self.file_registry.get(file_id)
        if not fm:
            return {"error": "File not found"}
        return IntegrityChecker.verify_file(fm, self.node_manager)