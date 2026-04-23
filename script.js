/**
 * script.js — Distributed File System Dashboard
 * Handles all frontend logic, API calls, live polling, and animations
 */

"use strict";

const API = "http://127.0.0.1:5000/api";
let lastEventTs    = 0;
let rfValue        = 2;
let pollTimer      = null;
let _pendingDelete = null;  // file_id currently awaiting confirmation

// ── Helpers ───────────────────────────────────────────────────────────────────

async function apiFetch(path, opts = {}) {
  const res = await fetch(API + path, {
    headers: { "Content-Type": "application/json", ...opts.headers },
    ...opts,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  return res.json();
}

function formatBytes(b) {
  if (b === 0) return "0 B";
  const k = 1024, sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(b) / Math.log(k));
  return parseFloat((b / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function timeAgo(ts) {
  const diff = Math.floor(Date.now() / 1000 - ts);
  if (diff < 5)  return "just now";
  if (diff < 60) return `${diff}s ago`;
  return `${Math.floor(diff / 60)}m ago`;
}

function formatDate(ts) {
  return new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

// ── Toast notifications ───────────────────────────────────────────────────────

function showToast(msg, type = "info", icon = "ℹ️") {
  const container = document.getElementById("toast-container");
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `<span>${icon}</span><span>${msg}</span>`;
  container.appendChild(el);
  setTimeout(() => {
    el.classList.add("removing");
    setTimeout(() => el.remove(), 300);
  }, 3500);
}

// ── Stats Strip ───────────────────────────────────────────────────────────────

async function refreshStats() {
  try {
    const s = await apiFetch("/system/stats");
    document.getElementById("stat-total-nodes").textContent   = s.total_nodes;
    document.getElementById("stat-online-nodes").textContent  = s.online_nodes;
    document.getElementById("stat-total-files").textContent   = s.total_files;
    document.getElementById("stat-at-risk").textContent       = s.files_at_risk;
    document.getElementById("stat-availability").textContent  = s.availability_pct + "%";
    document.getElementById("stat-bytes").textContent         = formatBytes(s.total_bytes_stored);
    rfValue = s.replication_factor;
    document.getElementById("rf-display").textContent = rfValue;
  } catch {/* silently fail */}
}

// ── Node Panel ────────────────────────────────────────────────────────────────

async function refreshNodes() {
  try {
    const nodes = await apiFetch("/nodes");
    renderNodes(nodes);
  } catch {/* silently fail */}
}

function renderNodes(nodes) {
  const grid = document.getElementById("node-grid");
  grid.innerHTML = "";

  if (!nodes.length) {
    grid.innerHTML = `<div class="empty-state"><div class="emoji">🌐</div><p>No nodes in cluster</p></div>`;
    return;
  }

  nodes.forEach(n => {
    const isOnline     = n.status === "online";
    const isRecovering = n.status === "recovering";
    const isOffline    = n.status === "offline";

    const card = document.createElement("div");
    card.className = `node-card ${n.status}`;
    card.id = `node-card-${n.node_id}`;

    const ringIcon = isOnline ? "🟢" : isRecovering ? "⚙️" : "🔴";
    const usedPct  = n.used_pct.toFixed(1);
    const lastHb   = timeAgo(n.last_heartbeat);

    card.innerHTML = `
      <div class="node-ring ${n.status}">${ringIcon}</div>
      <div class="node-id">${n.node_id}</div>
      <span class="node-status-badge ${n.status}">${n.status}</span>
      <div class="node-meta">
        <div>${n.chunks_stored} chunks</div>
        <div>${formatBytes(n.bytes_stored)}</div>
        <div style="color:var(--text-muted); font-size:0.63rem; margin-top:2px;">HB: ${lastHb}</div>
      </div>
      <div class="node-actions">
        ${isOffline
          ? `<button class="btn btn-success btn-sm" onclick="recoverNode('${n.node_id}')">Recover</button>
             <button class="btn btn-danger btn-sm" id="rm-btn-${n.node_id}" onclick="removeNode('${n.node_id}', this)">Remove</button>`
          : `<button class="btn btn-danger btn-sm" onclick="failNode('${n.node_id}')">Fail</button>`
        }
      </div>
    `;
    grid.appendChild(card);
  });
}

async function failNode(nodeId) {
  try {
    await apiFetch(`/node/${nodeId}/fail`, { method: "POST" });
    showToast(`Node ${nodeId} failed`, "error", "🔴");
    refresh();
  } catch (e) {
    showToast(e.message, "error", "❌");
  }
}

async function recoverNode(nodeId) {
  try {
    await apiFetch(`/node/${nodeId}/recover`, { method: "POST" });
    showToast(`Node ${nodeId} recovering…`, "warning", "⚙️");
    refresh();
  } catch (e) {
    showToast(e.message, "error", "❌");
  }
}

async function addNode() {
  try {
    const res = await apiFetch("/node/add", { method: "POST" });
    showToast(`Node ${res.node_id} added to cluster`, "success", "Added");
    refresh();
  } catch (e) {
    showToast(e.message, "error", "Error");
  }
}

let _pendingNodeRemove = null;

function removeNode(nodeId, btnEl) {
  // Two-click inline confirmation
  if (_pendingNodeRemove === nodeId) {
    _pendingNodeRemove = null;
    clearTimeout(btnEl._confirmTimer);
    _resetNodeRemoveBtn(btnEl);
    _doRemoveNode(nodeId);
    return;
  }

  if (_pendingNodeRemove) {
    const prev = document.querySelector('[data-node-confirming="true"]');
    if (prev) _resetNodeRemoveBtn(prev);
  }

  _pendingNodeRemove = nodeId;
  btnEl.dataset.nodeConfirming = "true";
  btnEl.textContent       = "Sure?";
  btnEl.style.background  = "var(--accent-red)";
  btnEl.style.color       = "#fff";
  btnEl.style.borderColor = "var(--accent-red)";

  // Pause polling so re-render doesn't orphan the button
  clearInterval(pollTimer);

  btnEl._confirmTimer = setTimeout(() => {
    _pendingNodeRemove = null;
    _resetNodeRemoveBtn(btnEl);
    pollTimer = setInterval(refresh, 2500);
  }, 4000);
}

function _resetNodeRemoveBtn(btn) {
  btn.textContent       = "Remove";
  btn.style.background  = "";
  btn.style.color       = "";
  btn.style.borderColor = "";
  delete btn.dataset.nodeConfirming;
}

async function _doRemoveNode(nodeId) {
  try {
    const res = await apiFetch(`/node/${nodeId}`, { method: "DELETE" });
    showToast(`${nodeId} permanently removed from cluster`, "info", "Removed");
  } catch (e) {
    showToast(e.message, "error", "Error");
  } finally {
    pollTimer = setInterval(refresh, 2500);
    refresh();
  }
}

// ── File Panel ────────────────────────────────────────────────────────────────

async function refreshFiles() {
  try {
    const files = await apiFetch("/files");
    renderFiles(files);
  } catch {/* silently fail */}
}

function getFileIcon(name) {
  const ext = name.split(".").pop().toLowerCase();
  const map = {
    pdf: "📄", txt: "📝", md: "📝",
    png: "🖼️", jpg: "🖼️", jpeg: "🖼️", gif: "🖼️", svg: "🖼️",
    mp4: "🎬", mkv: "🎬", avi: "🎬",
    mp3: "🎵", wav: "🎵",
    zip: "🗜️", tar: "🗜️", gz: "🗜️",
    py: "🐍", js: "📦", html: "🌐", css: "🎨",
    json: "📋", csv: "📊", xlsx: "📊",
  };
  return map[ext] || "📁";
}

function renderFiles(files) {
  const tbody = document.getElementById("file-tbody");
  tbody.innerHTML = "";

  if (!files.length) {
    tbody.innerHTML = `
      <tr><td colspan="6">
        <div class="empty-state">
          <div class="emoji">📭</div>
          <p>No files uploaded yet</p>
        </div>
      </td></tr>`;
    return;
  }

  files.forEach(f => {
    // Collect unique node IDs across all chunks
    const nodeSet = new Set();
    (f.chunks || []).forEach(c => c.node_ids.forEach(n => nodeSet.add(n)));

    const nodes = await_nodes_cached();
    const nodeStatusMap = {};
    nodes.forEach(n => nodeStatusMap[n.node_id] = n.status);

    const replicas = [...nodeSet].map(nid => {
      const alive = nodeStatusMap[nid] === "online";
      return `<span class="replica-pill ${alive ? '' : 'dead'}" title="${alive ? 'Online' : 'Offline'}">${nid}</span>`;
    }).join("");

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        <div class="file-name-cell">
          <span class="file-icon">${getFileIcon(f.name)}</span>
          <div>
            <div class="file-name" title="${f.name}">${f.name}</div>
            <div class="file-id-mono">${f.file_id.slice(0, 8)}…</div>
          </div>
        </div>
      </td>
      <td>${formatBytes(f.size)}</td>
      <td><span class="badge badge-blue">${f.num_chunks}</span></td>
      <td><div class="replica-pills">${replicas}</div></td>
      <td>${formatDate(f.upload_time)}</td>
      <td>
        <div class="flex-row gap-8">
          <button class="btn btn-ghost btn-sm" onclick="downloadFile('${f.file_id}', '${f.name}')" title="Download">&#11015;</button>
          <button class="btn btn-ghost btn-sm" onclick="checkIntegrity('${f.file_id}')" title="Verify integrity">Verify</button>
          <button class="btn btn-danger btn-sm" onclick="deleteFile('${f.file_id}', '${f.name}', this)" title="Click to delete (click again to confirm)">Delete</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

// Cache nodes for replica rendering
let _nodeCacheTs = 0;
let _nodeCache   = [];

function await_nodes_cached() {
  // Use cached version (refreshed in render cycle)
  return _nodeCache;
}

async function refreshNodeCache() {
  const data = await apiFetch("/nodes").catch(() => []);
  _nodeCache = data;
  _nodeCacheTs = Date.now();
}

async function downloadFile(fileId, name) {
  try {
    const res = await fetch(`${API}/download/${fileId}`);
    if (!res.ok) {
      const j = await res.json();
      throw new Error(j.error);
    }
    const blob = await res.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href = url; a.download = name;
    a.click();
    URL.revokeObjectURL(url);
    showToast(`Downloaded "${name}"`, "success", "⬇️");
  } catch (e) {
    showToast(e.message, "error", "❌");
  }
}

function deleteFile(fileId, name, btnEl) {
  // Two-click inline confirmation — avoids browser confirm() suppression issues
  if (_pendingDelete === fileId) {
    // Second click: confirmed — perform deletion
    _pendingDelete = null;
    clearTimeout(btnEl._confirmTimer);
    _resetDeleteBtn(btnEl, name);
    _doDelete(fileId, name);
    return;
  }

  // First click: ask for confirmation via button state
  if (_pendingDelete && _pendingDelete !== fileId) {
    // Cancel any other pending confirm
    const prev = document.querySelector('[data-confirming="true"]');
    if (prev) _resetDeleteBtn(prev, prev.dataset.origName);
  }

  _pendingDelete = fileId;
  btnEl.dataset.confirming = "true";
  btnEl.dataset.origName   = name;
  btnEl.textContent        = "Sure?";
  btnEl.style.background   = "var(--accent-red)";
  btnEl.style.color        = "#fff";
  btnEl.style.borderColor  = "var(--accent-red)";

  // Pause polling so the table doesn't re-render and orphan the button
  clearInterval(pollTimer);

  // Auto-cancel after 4 seconds
  btnEl._confirmTimer = setTimeout(() => {
    _pendingDelete = null;
    _resetDeleteBtn(btnEl, name);
    // Resume polling
    pollTimer = setInterval(refresh, 2500);
  }, 4000);
}

function _resetDeleteBtn(btn, name) {
  btn.textContent = "Delete";
  btn.style.background  = "";
  btn.style.color       = "";
  btn.style.borderColor = "";
  delete btn.dataset.confirming;
}

async function _doDelete(fileId, name) {
  try {
    await apiFetch(`/delete/${fileId}`, { method: "DELETE" });
    showToast(`Deleted "${name}"`, "info", "Deleted");
  } catch (e) {
    showToast(e.message, "error", "Error");
  } finally {
    // Always resume polling
    pollTimer = setInterval(refresh, 2500);
    refresh();
  }
}

async function checkIntegrity(fileId) {
  try {
    const res = await apiFetch(`/integrity/${fileId}`);
    let allOk = true;
    res.chunks.forEach(c => c.replicas.forEach(r => { if (!r.valid) allOk = false; }));
    if (allOk) {
      showToast("✅ All chunks verified — integrity OK", "success", "🔐");
    } else {
      showToast("⚠️ Integrity issues detected! Check console.", "error", "⚠️");
      console.warn("Integrity scan:", JSON.stringify(res, null, 2));
    }
  } catch (e) {
    showToast(e.message, "error", "❌");
  }
}

// ── Upload ────────────────────────────────────────────────────────────────────

function initUpload() {
  const zone    = document.getElementById("upload-zone");
  const input   = document.getElementById("file-input");
  const progress= document.getElementById("upload-progress");
  const bar     = document.getElementById("progress-fill");
  const label   = document.getElementById("progress-label");

  zone.addEventListener("dragover",  e => { e.preventDefault(); zone.classList.add("drag-over"); });
  zone.addEventListener("dragleave", () => zone.classList.remove("drag-over"));
  zone.addEventListener("drop", e => {
    e.preventDefault();
    zone.classList.remove("drag-over");
    const files = e.dataTransfer.files;
    if (files.length) uploadFiles(files);
  });

  input.addEventListener("change", () => {
    if (input.files.length) uploadFiles(input.files);
  });

  async function uploadFiles(fileList) {
    for (const file of fileList) {
      progress.classList.add("visible");
      bar.style.width = "0%";
      label.textContent = `Uploading ${file.name}…`;

      // Fake progress animation for UX
      let pct = 0;
      const ticker = setInterval(() => {
        pct = Math.min(pct + Math.random() * 12, 88);
        bar.style.width = pct + "%";
      }, 120);

      try {
        const form = new FormData();
        form.append("file", file);
        const res = await fetch(`${API}/upload`, { method: "POST", body: form });
        const j   = await res.json();
        clearInterval(ticker);

        if (!res.ok) throw new Error(j.error);

        bar.style.width = "100%";
        label.textContent = `✅ Uploaded: ${file.name} (${j.num_chunks} chunks)`;
        showToast(`"${file.name}" uploaded — ${j.num_chunks} chunk(s)`, "success", "📦");
        setTimeout(() => { progress.classList.remove("visible"); }, 1800);
        refresh();
      } catch (e) {
        clearInterval(ticker);
        bar.style.width = "100%";
        bar.style.background = "var(--accent-red)";
        label.textContent = `❌ Error: ${e.message}`;
        showToast(e.message, "error", "❌");
        setTimeout(() => {
          progress.classList.remove("visible");
          bar.style.background = "";
        }, 2500);
      }

      input.value = "";
    }
  }
}

// ── Event Log ─────────────────────────────────────────────────────────────────

const EVENT_ICONS = {
  upload:           "Upload",
  download:         "Download",
  node_failure:     "Failure",
  node_recovery:    "Recovery",
  node_add:         "Added",
  node_delete:      "Removed",
  replication:      "Replicated",
  replication_fail: "Rep.Fail",
  delete:           "Deleted",
};

async function refreshEvents() {
  try {
    const events = await apiFetch(`/events?since=${lastEventTs}`);
    if (!events.length) return;

    const log = document.getElementById("event-log");
    const wasAtBottom = log.scrollHeight - log.scrollTop <= log.clientHeight + 40;

    events.forEach(ev => {
      lastEventTs = Math.max(lastEventTs, ev.ts);
      const el = document.createElement("div");
      el.className = `event-entry ${ev.type}`;
      const icon = EVENT_ICONS[ev.type] || "ℹ️";
      el.innerHTML = `
        <span class="event-icon">${icon}</span>
        <div class="event-body">
          <div class="event-msg">${ev.message}</div>
          <div class="event-time">${formatDate(ev.ts)}</div>
        </div>
      `;
      log.appendChild(el);
    });

    if (wasAtBottom) log.scrollTop = log.scrollHeight;
  } catch {/* silently fail */}
}

// ── Replication Factor ────────────────────────────────────────────────────────

function initRFControl() {
  document.getElementById("rf-minus").addEventListener("click", async () => {
    if (rfValue <= 1) return;
    rfValue--;
    document.getElementById("rf-display").textContent = rfValue;
    await apiFetch("/system/replication-factor", {
      method: "POST",
      body: JSON.stringify({ factor: rfValue }),
    }).catch(() => {});
    showToast(`Replication factor set to ${rfValue}`, "info", "🔢");
  });

  document.getElementById("rf-plus").addEventListener("click", async () => {
    rfValue++;
    document.getElementById("rf-display").textContent = rfValue;
    await apiFetch("/system/replication-factor", {
      method: "POST",
      body: JSON.stringify({ factor: rfValue }),
    }).catch(() => {});
    showToast(`Replication factor set to ${rfValue}`, "info", "🔢");
  });
}

// ── Decorative network lines ──────────────────────────────────────────────────

function initNetworkLines() {
  const container = document.getElementById("network-lines");
  const count = 6;
  for (let i = 0; i < count; i++) {
    const el = document.createElement("div");
    el.className = "net-line";
    el.style.cssText = `
      top: ${Math.random() * 100}%;
      width: ${40 + Math.random() * 40}%;
      animation-duration: ${6 + Math.random() * 10}s;
      animation-delay: ${Math.random() * -12}s;
      opacity: ${0.3 + Math.random() * 0.4};
    `;
    container.appendChild(el);
  }
}

// ── Polling loop ──────────────────────────────────────────────────────────────

async function refresh() {
  await refreshNodeCache();
  await Promise.all([
    refreshStats(),
    refreshNodes(),
    refreshFiles(),
    refreshEvents(),
  ]);
}

function startPolling() {
  refresh();
  pollTimer = setInterval(refresh, 2500);
}

// Resume polling whenever page regains focus (extra safety net)
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && !pollTimer) {
    pollTimer = setInterval(refresh, 2500);
  }
});

// ── Init ──────────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  initNetworkLines();
  initUpload();
  initRFControl();

  document.getElementById("btn-add-node").addEventListener("click", addNode);
  document.getElementById("btn-refresh").addEventListener("click", refresh);

  startPolling();
});
