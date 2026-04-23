import requests
import json
import io
import time

BASE = "http://127.0.0.1:5000/api"

files_to_upload = [
    ("sample_analysis.py", b"""# sample_analysis.py
import os, json, hashlib

def analyze_data(filepath):
    results = {}
    with open(filepath) as f:
        data = json.load(f)
    for key, values in data.items():
        results[key] = {
            'count': len(values),
            'sum': sum(values),
            'avg': sum(values) / len(values),
            'max': max(values),
            'min': min(values)
        }
    return results

def compute_checksum(data):
    return hashlib.sha256(data.encode()).hexdigest()

if __name__ == '__main__':
    print('Running distributed analysis...')
"""),
    ("cluster_metrics.json", json.dumps({
        "cluster_metrics": {
            "node_1": {"cpu": 42, "memory": 68, "disk_io": 120, "network_in": 450, "network_out": 380},
            "node_2": {"cpu": 35, "memory": 55, "disk_io": 98,  "network_in": 320, "network_out": 290},
            "node_3": {"cpu": 61, "memory": 72, "disk_io": 145, "network_in": 510, "network_out": 430}
        },
        "replication_events": [
            {"ts": 1713340000, "chunk": "abc123", "from": "node_1", "to": "node_2", "status": "success"},
            {"ts": 1713340060, "chunk": "def456", "from": "node_2", "to": "node_3", "status": "success"}
        ],
        "integrity_checks": 47,
        "uptime_hours": 72.5,
        "files_replicated": 12,
        "total_chunks": 38
    }, indent=2).encode()),
    ("dfs_status_report.md", b"""# DFS Cluster Status Report

## Summary
Cluster operating at 100% availability. All 3 nodes online.
Data integrity checks passing on all 38 chunks.

## Node Health
| Node   | Status  | Chunks | Storage |
|--------|---------|--------|---------|
| node_1 | ONLINE  | 14     | 7.2 MB  |
| node_2 | ONLINE  | 13     | 6.8 MB  |
| node_3 | ONLINE  | 11     | 5.9 MB  |

## Fault Tolerance Events (Last 24h)
- 2 node failure simulations performed
- 0 data loss incidents
- Average recovery time: 4.2 seconds
- Integrity violations: 0
"""),
    ("audit_log.csv", b"""timestamp,node_id,event_type,file_id,status,latency_ms
2024-04-17T08:00:01,node_1,WRITE,f1a2b3c4,SUCCESS,12
2024-04-17T08:00:02,node_2,WRITE,f1a2b3c4,SUCCESS,14
2024-04-17T08:00:05,node_1,READ,f1a2b3c4,SUCCESS,8
2024-04-17T08:00:10,node_3,WRITE,f2e3d4c5,SUCCESS,11
2024-04-17T08:00:15,node_2,READ,f2e3d4c5,SUCCESS,9
2024-04-17T08:10:00,node_2,FAILURE_DETECTED,,,
2024-04-17T08:10:02,node_1,REPLICATE,f1a2b3c4,SUCCESS,22
2024-04-17T08:12:05,node_2,RECOVERY,,,
"""),
    ("dfs_config.ini", b"""[cluster]
name = production-dfs
replication_factor = 2
heartbeat_interval_sec = 2
heartbeat_timeout_sec = 6
chunk_size_kb = 512

[nodes]
node_1_host = 10.0.0.1
node_2_host = 10.0.0.2
node_3_host = 10.0.0.3

[integrity]
algorithm = SHA-256
verify_on_read = true
auto_repair = true

[logging]
level = INFO
retention_days = 30
"""),
]

print("=" * 50)
print("  DFS Sample Data Loader")
print("=" * 50)

print("\n[1] Uploading 5 sample files...")
for name, data in files_to_upload:
    r = requests.post(f"{BASE}/upload", files={"file": (name, io.BytesIO(data))})
    j = r.json()
    if r.ok:
        print(f"  OK  {name:30s}  chunks={j['num_chunks']}  id={j['file_id'][:8]}")
    else:
        print(f"  ERR {name}: {j}")

time.sleep(1)

print("\n[2] Simulating node_2 failure...")
r = requests.post(f"{BASE}/node/node_2/fail")
print("  ->", r.json())

time.sleep(2)

print("\n[3] Recovering node_2...")
r = requests.post(f"{BASE}/node/node_2/recover")
print("  ->", r.json())

time.sleep(1)

print("\n[4] Adding 4th node to cluster...")
r = requests.post(f"{BASE}/node/add")
print("  ->", r.json())

time.sleep(1)

print("\n[5] Final system stats:")
r = requests.get(f"{BASE}/system/stats")
stats = r.json()
for k, v in stats.items():
    print(f"  {k:30s} = {v}")

print("\nDone! Refresh the browser to see all changes.")
