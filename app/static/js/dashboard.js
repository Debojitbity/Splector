/**
 * Splector Dashboard — Client-Side Logic
 *
 * Manages:
 *   - SocketIO connection + reconnection
 *   - Progress bar updates with easing
 *   - Live terminal log streaming (500-line cap)
 *   - Stats card animated counters
 *   - Control button state machine
 *   - Config panel CRUD
 */

// =========================================================
// SOCKETIO CONNECTION
// =========================================================

const socket = io({ reconnection: true, reconnectionDelay: 1000 });

socket.on('connect', () => {
    appendTerminalLine('INFO', 'Connected to Splector server.', '');
    socket.emit('pipeline:status');
});

socket.on('disconnect', () => {
    appendTerminalLine('WARNING', 'Disconnected from server. Reconnecting...', '');
});


// =========================================================
// STATE
// =========================================================

let currentStatus = 'idle';
const TERMINAL_MAX_LINES = 500;

const stageTotals = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
const stageCurrent = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };


// =========================================================
// SOCKETIO EVENT LISTENERS
// =========================================================

// --- Pipeline status updates ---
socket.on('pipeline_status', (data) => {
    currentStatus = data.status;
    updateStatusPill(data.status);
    updateButtonStates(data.status);
});

// --- Pipeline idle reset ---
socket.on('pipeline_idle', (data) => {
    if (data.type === 'pipeline_idle') {
        // 1. Reset UI Badges
        const statusPill = document.getElementById('status-pill');
        const statusText = document.getElementById('status-text');
        if (statusPill && statusText) {
            statusPill.className = 'status-pill status-idle';
            statusText.textContent = 'Idle';
        }

        // 2. Reset Buttons
        const btnStart = document.getElementById('btn-start');
        const btnPause = document.getElementById('btn-pause');
        const btnStop = document.getElementById('btn-stop');
        if (btnStart) btnStart.disabled = false;
        if (btnPause) btnPause.disabled = true;
        if (btnStop) btnStop.disabled = true;

        // 3. Reset internal JS state trackers
        currentStatus = 'idle';
        window.isPipelineRunning = false;
        window.isPipelinePaused = false;
    }
});

// --- Stage start ---
socket.on('stage_start', (data) => {
    const { stage, total } = data;
    stageTotals[stage] = total;
    stageCurrent[stage] = 0;
    updateProgressBar(stage, 0, total);

    // Add shimmer to active bar
    const bar = document.getElementById(`stage-${stage}-bar`);
    if (bar) bar.classList.add('active');
});

// --- Progress updates ---
socket.on('progress', (data) => {
    const { stage, current, total } = data;
    stageTotals[stage] = total;
    stageCurrent[stage] = current;
    updateProgressBar(stage, current, total);
});

// --- Stage complete ---
socket.on('stage_complete', (data) => {
    const { stage } = data;
    const bar = document.getElementById(`stage-${stage}-bar`);
    if (bar) {
        bar.style.width = '100%';
        bar.classList.remove('active');
    }
    const label = document.getElementById(`stage-${stage}-label`);
    if (label) {
        label.textContent = `${stageTotals[stage].toLocaleString()} / ${stageTotals[stage].toLocaleString()}`;
        label.classList.add('text-emerald-400');
    }
});

// --- Log messages ---
socket.on('log', (data) => {
    appendTerminalLine(data.level, data.message, data.timestamp);
});

// --- Stats updates ---
socket.on('stats', (data) => {
    if (data.domains_loaded !== undefined) updateStat('stat-domains', data.domains_loaded || 0);
    if (data.urls_filtered !== undefined) updateStat('stat-urls', data.urls_filtered || 0);
    if (data.final_docs !== undefined) updateStat('stat-docs', data.final_docs || 0);

    // Phase 2: Document Processing stats
    if (data.docs_processed !== undefined) updateStat('stat-extracted', data.docs_processed || 0);
    if (data.docs_extracted !== undefined) updateStat('stat-extracted', data.docs_extracted || 0);

    // Telemetry Stats
    if (data.daemon_status !== undefined) {
        const el = document.getElementById('telemetry-status');
        if (el) el.textContent = data.daemon_status;
    }
    if (data.telemetry_backlog !== undefined) updateStat('telemetry-backlog', parseInt(data.telemetry_backlog) || 0);
    if (data.total_word_count !== undefined) updateStat('telemetry-words', parseInt(data.total_word_count) || 0);
    if (data.max_token_count !== undefined) updateStat('telemetry-tokens', parseInt(data.max_token_count) || 0);
    
    // Detailed Corpus Metrics
    if (data.total_files_processed !== undefined) {
        const el = document.getElementById('stat-total-files');
        if (el) el.textContent = parseInt(data.total_files_processed).toLocaleString() || "0";
    }
    
    if (data.min_file_size !== undefined && data.max_file_size !== undefined) {
        const el = document.getElementById('stat-file-size');
        if (el) el.textContent = `${formatBytes(parseInt(data.min_file_size) || 0)} - ${formatBytes(parseInt(data.max_file_size) || 0)}`;
    }
    
    if (data.min_word_count !== undefined && data.max_word_count !== undefined) {
        const el = document.getElementById('stat-word-count');
        if (el) el.textContent = `${(parseInt(data.min_word_count) || 0).toLocaleString()} - ${(parseInt(data.max_word_count) || 0).toLocaleString()}`;
    }
    
    if (data.min_token_count !== undefined && data.max_token_count !== undefined) {
        const el = document.getElementById('stat-token-count');
        if (el) el.textContent = `${(parseInt(data.min_token_count) || 0).toLocaleString()} - ${(parseInt(data.max_token_count) || 0).toLocaleString()}`;
    }
    
    if (data.count_english !== undefined) {
        const el = document.getElementById('stat-lang-en');
        if (el) el.textContent = parseInt(data.count_english).toLocaleString() || "0";
    }
    if (data.count_hindi !== undefined) {
        const el = document.getElementById('stat-lang-hi');
        if (el) el.textContent = parseInt(data.count_hindi).toLocaleString() || "0";
    }
    if (data.count_others !== undefined) {
        const el = document.getElementById('stat-lang-others');
        if (el) el.textContent = parseInt(data.count_others).toLocaleString() || "0";
    }
});


// =========================================================
// PROXY EXHAUSTION HANDLER
// =========================================================

// Inline Web Audio alert beep (880Hz square wave, 500ms) — no external file dependency
const ALERT_BEEP_B64 = (() => {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        return { ctx, canBeep: true };
    } catch (e) {
        return { ctx: null, canBeep: false };
    }
})();

function playAlertBeep() {
    if (!ALERT_BEEP_B64.canBeep || !ALERT_BEEP_B64.ctx) return;
    try {
        const ctx = ALERT_BEEP_B64.ctx;
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = 'square';
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        gain.gain.setValueAtTime(0.3, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.5);
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.start(ctx.currentTime);
        osc.stop(ctx.currentTime + 0.5);
    } catch (e) {
        // Silently fail if audio context is not available
    }
}

socket.on('proxy_exhausted', (data) => {
    appendTerminalLine('ERROR', data.message || 'All Cloudflare Workers exhausted!', '');
    playAlertBeep();

    // Show the modal
    const modal = document.getElementById('proxy-exhausted-modal');
    if (modal) modal.classList.remove('hidden');
});

// Modal button handlers (wired on DOMContentLoaded below)
function initProxyModal() {
    const modal = document.getElementById('proxy-exhausted-modal');
    const btnContinue = document.getElementById('btn-proxy-continue');
    const btnCancel = document.getElementById('btn-proxy-cancel');

    if (btnContinue) {
        btnContinue.addEventListener('click', () => {
            if (modal) modal.classList.add('hidden');
            socket.emit('proxy_decision', { action: 'continue_local' });
            appendTerminalLine('INFO', 'User approved local IP fallback.', '');
        });
    }

    if (btnCancel) {
        btnCancel.addEventListener('click', () => {
            if (modal) modal.classList.add('hidden');
            socket.emit('proxy_decision', { action: 'cancel' });
            appendTerminalLine('WARNING', 'User cancelled pipeline.', '');
        });
    }
}


// =========================================================
// PROGRESS BAR LOGIC
// =========================================================

function updateProgressBar(stage, current, total) {
    const bar = document.getElementById(`stage-${stage}-bar`);
    const label = document.getElementById(`stage-${stage}-label`);

    if (!bar || !label) return;

    const pct = total > 0 ? Math.min((current / total) * 100, 100) : 0;
    bar.style.width = `${pct}%`;
    label.textContent = `${current.toLocaleString()} / ${total.toLocaleString()}`;
}


// =========================================================
// TERMINAL LOG
// =========================================================

function appendTerminalLine(level, message, timestamp) {
    const content = document.getElementById('terminal-content');
    if (!content) return;

    const line = document.createElement('div');
    line.className = 'terminal-line';

    const ts = timestamp || new Date().toLocaleTimeString('en-US', { hour12: false });

    line.innerHTML = `<span class="log-timestamp">${ts}</span> <span class="log-${level}">[${level}]</span> <span class="log-message">${escapeHtml(message)}</span>`;

    content.appendChild(line);

    // Cap at max lines
    while (content.children.length > TERMINAL_MAX_LINES) {
        content.removeChild(content.firstChild);
    }

    // Auto-scroll to bottom (unless user has scrolled up)
    const terminal = document.getElementById('terminal');
    const isNearBottom = terminal.scrollHeight - terminal.scrollTop - terminal.clientHeight < 80;
    if (isNearBottom) {
        terminal.scrollTop = terminal.scrollHeight;
    }
}

function clearTerminal() {
    const content = document.getElementById('terminal-content');
    if (content) content.innerHTML = '';
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes, decimals = 2) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}


// =========================================================
// STATS CARD ANIMATED COUNTER
// =========================================================

function updateStat(elementId, newValue) {
    const el = document.getElementById(elementId);
    if (!el) return;

    const oldValue = parseInt(el.textContent.replace(/,/g, '')) || 0;

    if (newValue === oldValue) return;

    // Animate count-up
    const duration = 400;
    const start = performance.now();

    function step(now) {
        const elapsed = now - start;
        const progress = Math.min(elapsed / duration, 1);
        // Ease-out
        const eased = 1 - Math.pow(1 - progress, 3);
        const current = Math.round(oldValue + (newValue - oldValue) * eased);
        el.textContent = current.toLocaleString();

        if (progress < 1) {
            requestAnimationFrame(step);
        } else {
            el.textContent = newValue.toLocaleString();
            el.classList.add('stat-updated');
            setTimeout(() => el.classList.remove('stat-updated'), 300);
        }
    }

    requestAnimationFrame(step);
}


// =========================================================
// STATUS PILL
// =========================================================

function updateStatusPill(status) {
    const pill = document.getElementById('status-pill');
    const text = document.getElementById('status-text');

    if (!pill || !text) return;

    // Remove all status classes
    pill.className = 'status-pill';

    const statusMap = {
        'idle':      { class: 'status-idle',      label: 'Idle' },
        'running':   { class: 'status-running',   label: 'Running' },
        'paused':    { class: 'status-paused',    label: 'Paused' },
        'stopping':  { class: 'status-stopping',  label: 'Stopping' },
        'completed': { class: 'status-completed', label: 'Completed' },
        'cancelled': { class: 'status-idle',      label: 'Cancelled' },
        'error':     { class: 'status-error',     label: 'Error' },
    };

    const s = statusMap[status] || statusMap['idle'];
    pill.classList.add(s.class);
    text.textContent = s.label;
}


// =========================================================
// CONTROL BUTTON STATE MACHINE
// =========================================================

function updateButtonStates(status) {
    const btnStart = document.getElementById('btn-start');
    const btnPause = document.getElementById('btn-pause');
    const btnStop = document.getElementById('btn-stop');
    const btnSyncImport = document.getElementById('btn-sync-import');
    const btnSyncExport = document.getElementById('btn-sync-export');

    switch (status) {
        case 'idle':
        case 'completed':
        case 'cancelled':
        case 'error':
            btnStart.disabled = false;
            btnPause.disabled = true;
            btnStop.disabled = true;
            if (btnSyncImport) btnSyncImport.disabled = false;
            if (btnSyncExport) btnSyncExport.disabled = false;

            // Reset progress bars on idle
            if (status === 'idle' || status === 'cancelled' || status === 'error') {
                // Don't reset — keep final state visible
            }

            // Reset pause button text
            btnPause.innerHTML = `
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25v13.5m-7.5-13.5v13.5"/>
                </svg>
                Pause`;
            break;

        case 'running':
            btnStart.disabled = true;
            btnPause.disabled = false;
            btnStop.disabled = false;
            if (btnSyncImport) btnSyncImport.disabled = true;
            if (btnSyncExport) btnSyncExport.disabled = true;
            btnPause.innerHTML = `
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25v13.5m-7.5-13.5v13.5"/>
                </svg>
                Pause`;
            break;

        case 'paused':
            btnStart.disabled = true;
            btnPause.disabled = false;
            btnStop.disabled = false;
            if (btnSyncImport) btnSyncImport.disabled = true;
            if (btnSyncExport) btnSyncExport.disabled = true;
            btnPause.innerHTML = `
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M5.25 5.653c0-.856.917-1.398 1.667-.986l11.54 6.348a1.125 1.125 0 010 1.971l-11.54 6.347a1.125 1.125 0 01-1.667-.985V5.653z"/>
                </svg>
                Resume`;
            break;

        case 'stopping':
            btnStart.disabled = true;
            btnPause.disabled = true;
            btnStop.disabled = true;
            if (btnSyncImport) btnSyncImport.disabled = true;
            if (btnSyncExport) btnSyncExport.disabled = true;
            break;
    }
}


// =========================================================
// CONTROL ACTIONS
// =========================================================

function startPipeline() {
    // Reset progress bars
    for (let i = 1; i <= 5; i++) {
        const bar = document.getElementById(`stage-${i}-bar`);
        const label = document.getElementById(`stage-${i}-label`);
        if (bar) { bar.style.width = '0%'; bar.classList.remove('active'); }
        if (label) { label.textContent = '0 / 0'; label.classList.remove('text-emerald-400'); }
    }
    clearTerminal();
    
    const taskSelector = document.getElementById('task-selector');
    const selectedTask = taskSelector ? taskSelector.value : 'run_main_server';
    socket.emit('pipeline:start', { task: selectedTask });
}

function pausePipeline() {
    if (currentStatus === 'paused') {
        socket.emit('pipeline:resume');
    } else {
        socket.emit('pipeline:pause');
    }
}

function stopPipeline() {
    socket.emit('pipeline:stop');
}

function importFromCloud() {
    clearTerminal();
    appendTerminalLine('INFO', 'Requesting Cloud Sync (Import)...', '');
    socket.emit('pipeline:sync_import');
}

function exportToCloud() {
    clearTerminal();
    appendTerminalLine('INFO', 'Requesting Cloud Sync (Export)...', '');
    socket.emit('pipeline:sync_export');
}


// =========================================================
// CONFIG PANEL
// =========================================================

let configOpen = false;

function toggleConfig() {
    const overlay = document.getElementById('config-overlay');
    const panel = document.getElementById('config-panel');
    configOpen = !configOpen;

    if (configOpen) {
        overlay.classList.remove('hidden');
        requestAnimationFrame(() => {
            overlay.classList.remove('opacity-0');
            panel.classList.remove('translate-x-full');
        });
        loadConfig();
    } else {
        overlay.classList.add('opacity-0');
        panel.classList.add('translate-x-full');
        setTimeout(() => overlay.classList.add('hidden'), 300);
    }
}

async function loadConfig() {
    try {
        const res = await fetch('/api/config');
        const cfg = await res.json();

        // Populate workers textarea (array → newline-separated)
        const workers = cfg.cf_workers || [];
        document.getElementById('cfg-proxy').value = workers.join('\n');
        document.getElementById('cfg-concurrency').value = cfg.concurrency_limit || 80;
        document.getElementById('cfg-timeout').value = cfg.timeout_seconds || 15;
        document.getElementById('cfg-sheet').value = cfg.input_sheet || 'production';
        document.getElementById('cfg-ocr-workers').value = cfg.phase2_ocr_max_workers || 8;
        document.getElementById('cfg-download-concurrency').value = cfg.phase2_download_concurrency || 20;

        const toggle = document.getElementById('cfg-continue');
        const label = document.getElementById('cfg-continue-label');
        const active = cfg.continue_on_stage_error !== false;
        toggle.setAttribute('data-active', active);
        label.textContent = active ? 'Enabled' : 'Disabled';

        const toggleDl = document.getElementById('cfg-downloads');
        const labelDl = document.getElementById('cfg-downloads-label');
        const activeDl = cfg.enable_file_downloads !== false;
        toggleDl.setAttribute('data-active', activeDl);
        labelDl.textContent = activeDl ? 'Enabled' : 'Disabled';
    } catch (e) {
        appendTerminalLine('ERROR', `Failed to load config: ${e.message}`, '');
    }
}

function toggleContinueOnError() {
    const toggle = document.getElementById('cfg-continue');
    const label = document.getElementById('cfg-continue-label');
    const current = toggle.getAttribute('data-active') === 'true';
    toggle.setAttribute('data-active', !current);
    label.textContent = !current ? 'Enabled' : 'Disabled';
}

function toggleDownloads() {
    const toggle = document.getElementById('cfg-downloads');
    const label = document.getElementById('cfg-downloads-label');
    const current = toggle.getAttribute('data-active') === 'true';
    toggle.setAttribute('data-active', !current);
    label.textContent = !current ? 'Enabled' : 'Disabled';
}

async function saveConfig() {
    const payload = {
        // Parse workers textarea (newline-separated → array, filter empties)
        cf_workers: document.getElementById('cfg-proxy').value
            .split('\n')
            .map(s => s.trim())
            .filter(s => s.length > 0),
        concurrency_limit: parseInt(document.getElementById('cfg-concurrency').value) || 80,
        timeout_seconds: parseInt(document.getElementById('cfg-timeout').value) || 15,
        input_sheet: document.getElementById('cfg-sheet').value || 'production',
        phase2_ocr_max_workers: parseInt(document.getElementById('cfg-ocr-workers').value) || 8,
        phase2_download_concurrency: parseInt(document.getElementById('cfg-download-concurrency').value) || 20,
        continue_on_stage_error: document.getElementById('cfg-continue').getAttribute('data-active') === 'true',
        enable_file_downloads: document.getElementById('cfg-downloads').getAttribute('data-active') === 'true',
    };

    try {
        const res = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const result = await res.json();

        if (result.status === 'ok') {
            appendTerminalLine('INFO', 'Configuration saved successfully.', '');
            toggleConfig();
        }
    } catch (e) {
        appendTerminalLine('ERROR', `Failed to save config: ${e.message}`, '');
    }
}


// =========================================================
// STATS POLLING (fallback, supplements WebSocket)
// =========================================================

async function pollStats() {
    try {
        const res = await fetch('/api/stats');
        const data = await res.json();
        updateStat('stat-domains', data.domains_loaded || 0);
        updateStat('stat-urls', data.urls_filtered || 0);
        updateStat('stat-docs', data.final_docs || 0);
        updateStat('stat-extracted', data.docs_extracted || 0);

        if (data.daemon_status !== undefined) {
            const el = document.getElementById('telemetry-status');
            if (el) el.textContent = data.daemon_status;
        }
        if (data.telemetry_backlog !== undefined) updateStat('telemetry-backlog', parseInt(data.telemetry_backlog) || 0);
        if (data.total_word_count !== undefined) updateStat('telemetry-words', parseInt(data.total_word_count) || 0);
        if (data.max_token_count !== undefined) updateStat('telemetry-tokens', parseInt(data.max_token_count) || 0);

        if (data.total_files_processed !== undefined) {
            const el = document.getElementById('stat-total-files');
            if (el) el.textContent = parseInt(data.total_files_processed).toLocaleString() || "0";
        }
        
        if (data.min_file_size !== undefined && data.max_file_size !== undefined) {
            const el = document.getElementById('stat-file-size');
            if (el) el.textContent = `${formatBytes(parseInt(data.min_file_size) || 0)} - ${formatBytes(parseInt(data.max_file_size) || 0)}`;
        }
        
        if (data.min_word_count !== undefined && data.max_word_count !== undefined) {
            const el = document.getElementById('stat-word-count');
            if (el) el.textContent = `${(parseInt(data.min_word_count) || 0).toLocaleString()} - ${(parseInt(data.max_word_count) || 0).toLocaleString()}`;
        }
        
        if (data.min_token_count !== undefined && data.max_token_count !== undefined) {
            const el = document.getElementById('stat-token-count');
            if (el) el.textContent = `${(parseInt(data.min_token_count) || 0).toLocaleString()} - ${(parseInt(data.max_token_count) || 0).toLocaleString()}`;
        }
        
        if (data.count_english !== undefined) {
            const el = document.getElementById('stat-lang-en');
            if (el) el.textContent = parseInt(data.count_english).toLocaleString() || "0";
        }
        if (data.count_hindi !== undefined) {
            const el = document.getElementById('stat-lang-hi');
            if (el) el.textContent = parseInt(data.count_hindi).toLocaleString() || "0";
        }
        if (data.count_others !== undefined) {
            const el = document.getElementById('stat-lang-others');
            if (el) el.textContent = parseInt(data.count_others).toLocaleString() || "0";
        }

    } catch (e) {
        // Silently fail — WebSocket will handle it
    }
}

// Poll stats every 5s as a fallback
setInterval(pollStats, 5000);

// Initial stats load
pollStats();

// =========================================================
// GHOST PURGE (TELEMETRY SYNC)
// =========================================================

document.addEventListener('DOMContentLoaded', () => {
    // --- Proxy Modal ---
    initProxyModal();

    // --- Telemetry Sync ---
    const btnSyncTelemetry = document.getElementById('btn-sync-telemetry');
    if (btnSyncTelemetry) {
        btnSyncTelemetry.addEventListener('click', async () => {
            const icon = btnSyncTelemetry.querySelector('svg');
            if (icon) icon.classList.add('animate-spin');
            
            try {
                const res = await fetch('/api/telemetry/sync', { method: 'POST' });
                const result = await res.json();
                
                if (result.status === 'success') {
                    appendTerminalLine('INFO', `Telemetry Sync: ${result.message}`, '');
                    pollStats();
                } else {
                    appendTerminalLine('ERROR', `Telemetry Sync Failed: ${result.message}`, '');
                }
            } catch (e) {
                appendTerminalLine('ERROR', `Telemetry Sync Error: ${e.message}`, '');
            } finally {
                if (icon) icon.classList.remove('animate-spin');
            }
        });
    }
});
