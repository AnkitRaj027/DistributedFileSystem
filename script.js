/**
 * DistributedFS v2.0 — Premium Client Logic
 * Handles real-time topology, concurrent upload pipeline, and system monitoring.
 */

// ── State Management ─────────────────────────────────────────────────────────

const state = {
    nodes: [],
    files: [],
    events: [],
    stats: {},
    lastEventTs: 0,
    activeSection: 'overview',
    network: null, // vis.js instance
    nodesDataset: null,
    edgesDataset: null,
    isRefreshing: false
};

const API_BASE = '/api';
const REFRESH_INTERVAL = 2500;

// ── Initialization ───────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    initNavigation();
    initUploadZone();
    initTopology();
    startRefreshCycle();

    // Global listeners
    document.getElementById('btn-refresh').addEventListener('click', manualRefresh);
    document.getElementById('btn-add-node').addEventListener('click', addNode);
    document.getElementById('rf-minus').addEventListener('click', () => updateRF(-1));
    document.getElementById('rf-plus').addEventListener('click', () => updateRF(1));
    document.getElementById('file-search').addEventListener('input', handleSearch);
    document.getElementById('btn-clear-events').addEventListener('click', clearEvents);
    document.getElementById('modal-close').addEventListener('click', closeModal);
    document.getElementById('btn-topo-fit').addEventListener('click', () => state.network.fit({ animation: true }));
});

// ── Navigation ───────────────────────────────────────────────────────────────

function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item');
    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const section = item.getAttribute('data-section');
            switchSection(section);
        });
    });
}

function switchSection(sectionId) {
    state.activeSection = sectionId;
    
    // Update Sidebar
    document.querySelectorAll('.nav-item').forEach(el => {
        el.classList.toggle('active', el.getAttribute('data-section') === sectionId);
    });

    // Update Content
    document.querySelectorAll('.page-section').forEach(el => {
        el.classList.toggle('active', el.id === `section-${sectionId}`);
    });

    // Update Header
    const titles = {
        overview: ['System Overview', 'Real-time cluster health and statistics'],
        topology: ['Cluster Topology', 'Interactive node & data distribution graph'],
        files: ['File Registry', 'Manage distributed objects and replicas'],
        upload: ['Upload Center', 'Secure parallel ingestion pipeline'],
        events: ['Event Streaming', 'Live system activity and audit log']
    };

    const [title, subtitle] = titles[sectionId] || ['DistributedFS', ''];
    document.getElementById('page-title').innerText = title;
    document.getElementById('page-subtitle').innerText = subtitle;

    if (sectionId === 'topology' && state.network) {
        setTimeout(() => {
            const container = document.getElementById('topology-graph');
            if (container) {
                state.network.setSize(container.offsetWidth + 'px', container.offsetHeight + 'px');
                state.network.redraw();
                state.network.fit({ animation: false });
            }
        }, 150);
    }
}

// ── Topology Graph (vis.js) ─────────────────────────────────────────────────

function initTopology() {
    state.nodesDataset = new vis.DataSet([]);
    state.edgesDataset = new vis.DataSet([]);

    const container = document.getElementById('topology-graph');
    const data = { nodes: state.nodesDataset, edges: state.edgesDataset };
    const options = {
        nodes: {
            shape: 'dot',
            size: 25,
            font: { color: '#94a3b8', size: 12, face: 'Inter' },
            borderWidth: 2,
            shadow: { enabled: true, color: 'rgba(0,0,0,0.5)', size: 10 }
        },
        edges: {
            width: 2,
            color: { color: '#334155', highlight: '#6366f1' },
            smooth: { type: 'continuous' }
        },
        physics: {
            forceAtlas2Based: { gravitationalConstant: -50, centralGravity: 0.01, springLength: 100, springConstant: 0.08 },
            maxVelocity: 50,
            solver: 'forceAtlas2Based',
            timestep: 0.35,
            stabilization: { iterations: 150 }
        },
        interaction: { hover: true, tooltipDelay: 200 }
    };

    state.network = new vis.Network(container, data, options);
}

function updateTopology(nodes) {
    const nodesUpdate = [];
    const edgesUpdate = [];

    // Master Node (Virtual)
    if (!state.nodesDataset.get('master')) {
        nodesUpdate.push({
            id: 'master',
            label: 'MASTER',
            title: 'Master Coordinator',
            color: { background: '#6366f1', border: '#818cf8' },
            size: 35,
            icon: { face: 'Inter', code: '\uf0e8', color: '#fff' }
        });
    }

    nodes.forEach(node => {
        let color = '#10b981'; // online
        if (node.status === 'offline') color = '#ef4444';
        if (node.status === 'recovering') color = '#f59e0b';

        nodesUpdate.push({
            id: node.node_id,
            label: node.node_id,
            title: `${node.node_id}\nStorage: ${formatBytes(node.bytes_stored)} / ${formatBytes(node.total_capacity)}\nStatus: ${node.status}`,
            color: { background: color, border: 'rgba(255,255,255,0.1)' }
        });

        const edgeId = `m-${node.node_id}`;
        if (!state.edgesDataset.get(edgeId)) {
            edgesUpdate.push({ id: edgeId, from: 'master', to: node.node_id, dashes: node.status === 'offline' });
        } else {
            state.edgesDataset.update({ id: edgeId, dashes: node.status === 'offline' });
        }
    });

    state.nodesDataset.update(nodesUpdate);
    state.edgesDataset.update(edgesUpdate);

    // Remove defunct nodes
    const currentIds = nodes.map(n => n.node_id).concat(['master']);
    state.nodesDataset.getIds().forEach(id => {
        if (!currentIds.includes(id)) {
            state.nodesDataset.remove(id);
            state.edgesDataset.remove(`m-${id}`);
        }
    });
}

// ── Refresh Cycle ───────────────────────────────────────────────────────────

async function manualRefresh() {
    const btn = document.getElementById('btn-refresh');
    btn.classList.add('rotating');
    await refreshAll();
    setTimeout(() => btn.classList.remove('rotating'), 500);
    showToast('System data refreshed');
}

function startRefreshCycle() {
    refreshAll();
    setInterval(refreshAll, REFRESH_INTERVAL);
}

async function refreshAll() {
    if (state.isRefreshing) return;
    state.isRefreshing = true;

    try {
        const [nodes, files, stats, events] = await Promise.all([
            fetch(`${API_BASE}/nodes`).then(r => r.json()),
            fetch(`${API_BASE}/files`).then(r => r.json()),
            fetch(`${API_BASE}/system/stats`).then(r => r.json()),
            fetch(`${API_BASE}/events?since=${state.lastEventTs}`).then(r => r.json())
        ]);

        state.nodes = nodes;
        state.files = files;
        state.stats = stats;
        
        if (events.length > 0) {
            state.events = [...events, ...state.events].slice(0, 200);
            state.lastEventTs = events[0].ts;
            updateEventLog(events);
        }

        updateUI();
    } catch (err) {
        console.error('Refresh failed:', err);
    } finally {
        state.isRefreshing = false;
    }
}

// ── UI Updates ──────────────────────────────────────────────────────────────

function updateUI() {
    // Stats
    document.getElementById('stat-total-nodes').innerText = state.stats.total_nodes;
    document.getElementById('stat-online-nodes').innerText = state.stats.online_nodes;
    document.getElementById('stat-availability').innerText = `${state.stats.availability_pct}%`;
    document.getElementById('stat-total-files').innerText = state.stats.total_files;
    document.getElementById('stat-at-risk').innerText = state.stats.files_at_risk;
    document.getElementById('stat-bytes').innerText = formatBytes(state.stats.total_bytes_stored);
    document.getElementById('rf-display').innerText = state.stats.replication_factor;

    // Chips
    document.getElementById('chip-encrypt').style.opacity = state.stats.encryption_enabled ? '1' : '0.3';
    document.getElementById('chip-compress').style.opacity = state.stats.compression_enabled ? '1' : '0.3';

    // Node Grid
    renderNodeGrid();

    // File Table
    renderFileTable();

    // Topology
    updateTopology(state.nodes);
}

function renderNodeGrid() {
    const grid = document.getElementById('node-grid');
    grid.innerHTML = state.nodes.map(node => `
        <div class="node-card">
            <div class="node-header">
                <span class="node-id">${node.node_id}</span>
                <span class="status-badge status-${node.status}">${node.status}</span>
            </div>
            <div class="node-stats">
                <div class="stat-item">
                    <span class="stat-label">Chunks</span>
                    <span class="stat-num">${node.chunks_stored}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Storage</span>
                    <span class="stat-num">${formatBytes(node.bytes_stored)}</span>
                </div>
            </div>
            <div class="storage-bar-wrap">
                <div class="storage-bar-label">
                    <span>Utilization</span>
                    <span>${node.used_pct}%</span>
                </div>
                <div class="storage-track">
                    <div class="storage-fill" style="width: ${node.used_pct}%"></div>
                </div>
            </div>
            <div class="node-actions">
                ${node.status === 'online' 
                    ? `<button class="btn btn-danger btn-sm" onclick="failNode('${node.node_id}')">Fail</button>` 
                    : `<button class="btn btn-primary btn-sm" onclick="recoverNode('${node.node_id}')">Recover</button>`
                }
                <button class="btn btn-ghost btn-sm" onclick="deleteNode('${node.node_id}')" ${node.status === 'online' ? 'disabled' : ''}>Remove</button>
            </div>
        </div>
    `).join('');
}

function renderFileTable() {
    const tbody = document.getElementById('file-tbody');
    const searchTerm = document.getElementById('file-search').value.toLowerCase();
    
    const filtered = state.files.filter(f => f.name.toLowerCase().includes(searchTerm));

    if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7"><div class="empty-state">No files found</div></td></tr>';
        return;
    }

    tbody.innerHTML = filtered.map(file => {
        const nodes = [...new Set(file.chunks.flatMap(c => c.node_ids))];
        return `
            <tr>
                <td>
                    <div class="file-name-cell">
                        <div class="file-icon">📄</div>
                        <div class="file-info">
                            <span class="file-name">${file.name}</span>
                            <span class="file-id-sub">${file.file_id.slice(0, 8)}...</span>
                        </div>
                    </div>
                </td>
                <td>${formatBytes(file.size)}</td>
                <td>${file.num_chunks}</td>
                <td>
                    <span class="badge ${file.chunks[0]?.compressed ? 'badge-blue' : ''}">
                        ${file.chunks[0]?.compressed ? 'zlib Active' : 'None'}
                    </span>
                </td>
                <td>
                    <div class="node-badges">
                        ${nodes.map(nid => `<span class="node-badge">${nid}</span>`).join('')}
                    </div>
                </td>
                <td class="text-muted">${new Date(file.upload_time * 1000).toLocaleTimeString()}</td>
                <td>
                    <div style="display:flex; gap:8px;">
                        <button class="icon-btn btn-sm" title="Download" onclick="downloadFile('${file.file_id}')">⬇</button>
                        <button class="icon-btn btn-sm" title="Integrity Scan" onclick="verifyIntegrity('${file.file_id}')">🛡</button>
                        <button class="icon-btn btn-sm" title="Delete" onclick="deleteFile('${file.file_id}')">✕</button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

function updateEventLog(newEvents) {
    const log = document.getElementById('event-log');
    const badge = document.getElementById('nav-event-count');
    
    if (log.querySelector('.empty-state')) log.innerHTML = '';

    const html = newEvents.map(ev => `
        <div class="event-item">
            <div class="event-time">${new Date(ev.ts * 1000).toLocaleTimeString([], {hour12:false})}</div>
            <div class="event-content">
                <div class="event-type type-${ev.type}">${ev.type.replace('_', ' ')}</div>
                <div class="event-msg">${ev.message}</div>
            </div>
        </div>
    `).join('');

    log.insertAdjacentHTML('afterbegin', html);
    badge.innerText = parseInt(badge.innerText || 0) + newEvents.length;
}

// ── File Operations ──────────────────────────────────────────────────────────

function initUploadZone() {
    const zone = document.getElementById('upload-zone');
    const input = document.getElementById('file-input');

    zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('dragover'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('dragover');
        handleFiles(e.dataTransfer.files);
    });
    input.addEventListener('change', () => handleFiles(input.files));
}

async function handleFiles(files) {
    if (files.length === 0) return;
    
    switchSection('upload');
    const progress = document.getElementById('upload-progress');
    progress.classList.remove('hidden');

    for (const file of files) {
        document.getElementById('progress-filename').innerText = `Uploading: ${file.name}`;
        await uploadFile(file);
    }

    setTimeout(() => {
        progress.classList.add('hidden');
        resetUploadSteps();
        manualRefresh();
    }, 2000);
}

async function uploadFile(file) {
    const steps = ['step-chunk', 'step-compress', 'step-encrypt', 'step-replicate', 'step-done'];
    const fill = document.getElementById('progress-fill');
    
    const updateStep = (idx) => {
        steps.forEach((s, i) => {
            const el = document.getElementById(s);
            el.classList.toggle('active', i === idx);
        });
        fill.style.width = `${(idx / (steps.length - 1)) * 100}%`;
        document.getElementById('progress-pct').innerText = `${Math.round((idx / (steps.length - 1)) * 100)}%`;
    };

    try {
        updateStep(0); // Chunking
        await sleep(400);
        updateStep(1); // Compressing
        await sleep(300);
        updateStep(2); // Encrypting
        await sleep(300);
        updateStep(3); // Replicating

        const formData = new FormData();
        formData.append('file', file);

        const res = await fetch(`${API_BASE}/upload`, { method: 'POST', body: formData });
        if (!res.ok) throw new Error(await res.text());

        updateStep(4); // Done
        showToast(`Successfully uploaded ${file.name}`, 'success');
    } catch (err) {
        showToast(`Upload failed: ${err.message}`, 'error');
    }
}

function resetUploadSteps() {
    const steps = ['step-chunk', 'step-compress', 'step-encrypt', 'step-replicate', 'step-done'];
    steps.forEach(s => document.getElementById(s).classList.remove('active'));
    document.getElementById('progress-fill').style.width = '0%';
}

async function downloadFile(id) {
    window.location.href = `${API_BASE}/download/${id}`;
    showToast('Download started');
}

async function deleteFile(id) {
    if (!confirm('Permanently delete this file?')) return;
    const res = await fetch(`${API_BASE}/delete/${id}`, { method: 'DELETE' });
    if (res.ok) {
        showToast('File deleted');
        manualRefresh();
    }
}

async function verifyIntegrity(id) {
    showToast('Starting integrity scan...');
    const res = await fetch(`${API_BASE}/integrity/${id}`);
    const data = await res.json();
    
    const body = document.getElementById('modal-body');
    body.innerHTML = `
        <div class="integrity-report">
            <h4>File ID: ${data.file_id}</h4>
            <div class="chunk-list" style="margin-top:20px;">
                ${data.chunks.map(c => `
                    <div style="margin-bottom:15px; padding:12px; background:rgba(255,255,255,0.03); border-radius:8px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
                            <strong>Chunk #${c.index}</strong>
                            <code style="font-size:0.7rem;">${c.chunk_id}</code>
                        </div>
                        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
                            ${c.replicas.map(r => `
                                <div style="font-size:0.8rem; color:${r.valid ? 'var(--status-green)' : 'var(--status-red)'}">
                                    ${r.node}: ${r.valid ? '✓ Valid' : '✕ ' + r.msg}
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
    openModal('Integrity Report');
}

// ── Node Actions ────────────────────────────────────────────────────────────

async function addNode() {
    const res = await fetch(`${API_BASE}/node/add`, { method: 'POST' });
    if (res.ok) {
        showToast('New storage node added');
        manualRefresh();
    }
}

async function failNode(id) {
    const res = await fetch(`${API_BASE}/node/${id}/fail`, { method: 'POST' });
    if (res.ok) {
        showToast(`Node ${id} simulated failure`);
        manualRefresh();
    }
}

async function recoverNode(id) {
    const res = await fetch(`${API_BASE}/node/${id}/recover`, { method: 'POST' });
    if (res.ok) {
        showToast(`Node ${id} recovery started`);
        manualRefresh();
    }
}

async function deleteNode(id) {
    const res = await fetch(`${API_BASE}/node/${id}`, { method: 'DELETE' });
    const data = await res.json();
    if (res.ok) {
        showToast(data.message || `Node ${id} removed`);
        manualRefresh();
    } else {
        showToast(data.error, 'error');
    }
}

async function updateRF(delta) {
    const current = state.stats.replication_factor;
    const next = Math.max(1, current + delta);
    if (next === current) return;

    const res = await fetch(`${API_BASE}/system/replication-factor`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ factor: next })
    });
    if (res.ok) {
        showToast(`Replication factor updated to ${next}`);
        manualRefresh();
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function formatBytes(bytes, decimals = 2) {
    if (!+bytes) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

function handleSearch() {
    renderFileTable();
}

function clearEvents() {
    document.getElementById('event-log').innerHTML = '<div class="empty-state" style="padding:40px;"><div class="empty-icon">📻</div><p>Waiting for cluster events…</p></div>';
    document.getElementById('nav-event-count').innerText = '0';
    state.events = [];
}

function showToast(msg, type = 'success') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `<span>${type === 'success' ? '✓' : '✕'}</span> ${msg}`;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
}

function openModal(title) {
    document.getElementById('modal-title').innerText = title;
    document.getElementById('modal-overlay').style.display = 'flex';
}

function closeModal() {
    document.getElementById('modal-overlay').style.display = 'none';
}

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
