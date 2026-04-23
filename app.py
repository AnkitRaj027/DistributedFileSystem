"""
app.py — Master Coordinator Flask Server
Exposes REST API for all DFS operations
"""

import time
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from dfs_core import DistributedFileSystem
import io

app = Flask(__name__, static_folder=".", static_url_path="")
CORS(app)

# ── Bootstrap DFS with 3 nodes, replication factor = 2 ────────────────────────
dfs = DistributedFileSystem(replication_factor=2, initial_nodes=3)


# ─────────────────────────────────────────────────────────────────────────────
# Root — Serve the dashboard
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return app.send_static_file("index.html")


# ─────────────────────────────────────────────────────────────────────────────
# Node API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/nodes", methods=["GET"])
def get_nodes():
    return jsonify(dfs.node_manager.to_dict())


@app.route("/api/node/add", methods=["POST"])
def add_node():
    info = dfs.add_node()
    return jsonify({"success": True, "node_id": info.node_id}), 201


@app.route("/api/node/<node_id>/fail", methods=["POST"])
def fail_node(node_id):
    ok = dfs.fail_node(node_id)
    if not ok:
        return jsonify({"error": "Node not found"}), 404
    return jsonify({"success": True, "node_id": node_id, "status": "offline"})


@app.route("/api/node/<node_id>/recover", methods=["POST"])
def recover_node(node_id):
    ok = dfs.recover_node(node_id)
    if not ok:
        return jsonify({"error": "Node not found"}), 404
    return jsonify({"success": True, "node_id": node_id, "status": "recovering"})


@app.route("/api/node/<node_id>", methods=["DELETE"])
def delete_node(node_id):
    node = dfs.node_manager.get_node(node_id)
    if not node:
        return jsonify({"error": "Node not found"}), 404
    if node.status.value == "online":
        return jsonify({"error": "Cannot remove an online node. Fail it first."}), 400
    ok, msg = dfs.delete_node(node_id)
    if not ok:
        return jsonify({"error": msg}), 400
    return jsonify({"success": True, "node_id": node_id, "message": msg})


# ─────────────────────────────────────────────────────────────────────────────
# File API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/files", methods=["GET"])
def list_files():
    return jsonify(dfs.file_registry.to_dict())


@app.route("/api/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f        = request.files["file"]
    filename = f.filename or "unnamed"
    data     = f.read()

    if not data:
        return jsonify({"error": "Empty file"}), 400

    online = dfs.node_manager.online_nodes()
    if not online:
        return jsonify({"error": "No online nodes available"}), 503

    meta = dfs.upload_file(filename, data)
    return jsonify({
        "success":    True,
        "file_id":    meta.file_id,
        "name":       meta.name,
        "size":       meta.size,
        "num_chunks": meta.num_chunks,
        "checksum":   meta.checksum,
    }), 201


@app.route("/api/download/<file_id>", methods=["GET"])
def download_file(file_id):
    data, name_or_err = dfs.download_file(file_id)
    if data is None:
        return jsonify({"error": name_or_err}), 404
    return send_file(
        io.BytesIO(data),
        download_name=name_or_err,
        as_attachment=True,
    )


@app.route("/api/delete/<file_id>", methods=["DELETE"])
def delete_file(file_id):
    ok = dfs.delete_file(file_id)
    if not ok:
        return jsonify({"error": "File not found"}), 404
    return jsonify({"success": True, "file_id": file_id})


@app.route("/api/integrity/<file_id>", methods=["GET"])
def integrity_check(file_id):
    result = dfs.integrity_scan(file_id)
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# System API
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/system/stats", methods=["GET"])
def system_stats():
    return jsonify(dfs.system_stats())


@app.route("/api/events", methods=["GET"])
def get_events():
    since = float(request.args.get("since", 0))
    return jsonify(dfs.get_events(since))


@app.route("/api/system/replication-factor", methods=["POST"])
def set_replication_factor():
    body = request.get_json(silent=True) or {}
    rf   = body.get("factor")
    if not isinstance(rf, int) or rf < 1:
        return jsonify({"error": "factor must be a positive integer"}), 400
    dfs.replication_manager.replication_factor = rf
    dfs.replication_factor = rf
    return jsonify({"success": True, "replication_factor": rf})


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n[DFS] Distributed File System -- Master Coordinator")
    print("   Dashboard -> http://127.0.0.1:5000\n")
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
