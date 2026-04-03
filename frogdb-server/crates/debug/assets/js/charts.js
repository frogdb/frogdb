// FrogDB Debug Dashboard - Chart Management
// Uses uPlot for time-series visualization with client-side rate computation.

(function() {
    'use strict';

    const MAX_POINTS = 300; // 10 min at 2s interval
    const POLL_INTERVAL = 2000; // 2 seconds

    // Dark theme colors matching website (Starlight Rapide + FrogDB green)
    const COLORS = {
        green:  '#6BAA3D',
        orange: '#d29922',
        blue:   '#58a6ff',
        purple: '#bc8cff',
        lime:   '#8DC63F',
        red:    '#f85149',
        cyan:   '#39d353',
        yellow: '#e3b341',
    };

    const GRID_COLOR = 'rgba(48, 54, 61, 0.6)';
    const AXIS_COLOR = '#8b949e';

    // Compute local timezone abbreviation once at load time
    const TZ_ABBR = new Date().toLocaleTimeString('en', { timeZoneName: 'short' }).split(' ').pop();

    // Time display mode: false = local, true = UTC
    let useUTC = false;

    // Rolling data buffer
    let history = [];
    let charts = {};
    let pollTimer = null;
    let initialized = false;

    // Helper to create a series with matching point fill color
    function series(label, color, opts) {
        return Object.assign({
            label,
            stroke: color,
            width: 2,
            points: { show: true, fill: color, size: 4 },
        }, opts);
    }

    // Base uPlot options shared by all charts
    function baseOpts(title, width, height) {
        return {
            width: width,
            height: height,
            cursor: { show: true, drag: { x: false, y: false }, points: { fill: '#fff', size: 6 } },
            legend: { show: true, live: true },
            axes: [
                {
                    stroke: AXIS_COLOR,
                    grid: { stroke: GRID_COLOR, width: 1 },
                    ticks: { stroke: GRID_COLOR, width: 1 },
                    values: (u, vals) => {
                        const latest = vals[vals.length - 1];
                        let prev = null;
                        return vals.map(v => {
                            const diff = Math.round(latest - v);
                            const label = diff < 60 ? 'now' : `-${Math.floor(diff / 60)}m`;
                            if (label === prev) return '';
                            prev = label;
                            return label;
                        });
                    },
                },
                {
                    stroke: AXIS_COLOR,
                    grid: { stroke: GRID_COLOR, width: 1 },
                    ticks: { stroke: GRID_COLOR, width: 1 },
                    size: 60,
                },
            ],
        };
    }

    // Format bytes for axis labels
    function fmtBytes(val) {
        if (val >= 1073741824) return (val / 1073741824).toFixed(1) + ' GB';
        if (val >= 1048576) return (val / 1048576).toFixed(1) + ' MB';
        if (val >= 1024) return (val / 1024).toFixed(1) + ' KB';
        return val.toFixed(0) + ' B';
    }

    // Format timestamp for legend (absolute time + timezone)
    function fmtTime(u, v) {
        if (v == null) return '—';
        const d = new Date(v * 1000);
        if (useUTC) {
            const hh = String(d.getUTCHours()).padStart(2, '0');
            const mm = String(d.getUTCMinutes()).padStart(2, '0');
            const ss = String(d.getUTCSeconds()).padStart(2, '0');
            return `${hh}:${mm}:${ss} UTC`;
        }
        const hh = String(d.getHours()).padStart(2, '0');
        const mm = String(d.getMinutes()).padStart(2, '0');
        const ss = String(d.getSeconds()).padStart(2, '0');
        return `${hh}:${mm}:${ss} ${TZ_ABBR}`;
    }

    // Format rate (per second, with decimals)
    function fmtRate(val) {
        if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M/s';
        if (val >= 1000) return (val / 1000).toFixed(1) + 'K/s';
        return val.toFixed(1) + '/s';
    }

    // Format integer rate (per second, no decimals for small values)
    function fmtIntRate(val) {
        if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M/s';
        if (val >= 1000) return (val / 1000).toFixed(1) + 'K/s';
        return Math.round(val) + '/s';
    }

    // Format percentage
    function fmtPct(val) {
        return val.toFixed(1) + '%';
    }

    // Compute rate between two consecutive counter values
    function computeRates(timestamps, counterValues) {
        const rates = [null]; // first point has no rate
        for (let i = 1; i < timestamps.length; i++) {
            const dt = timestamps[i] - timestamps[i - 1];
            if (dt > 0) {
                const dv = counterValues[i] - counterValues[i - 1];
                rates.push(Math.max(0, dv / dt));
            } else {
                rates.push(0);
            }
        }
        return rates;
    }

    // Build chart data arrays from history
    function buildData() {
        const ts = history.map(h => h.timestamp);

        // Commands/s (rate from counter)
        const cmdTotal = history.map(h => h.commands_total);
        const cmdRate = computeRates(ts, cmdTotal);

        // Memory (gauges)
        const memUsed = history.map(h => h.memory_used_bytes);
        const memRss = history.map(h => h.memory_rss_bytes);
        const memMax = history.map(h => h.memory_max_bytes || null);

        // Connections (gauge)
        const conns = history.map(h => h.connections_current);

        // Keys (gauge)
        const keys = history.map(h => h.keys_total);

        // Network I/O (rates from counters)
        const netIn = history.map(h => h.net_input_bytes_total);
        const netOut = history.map(h => h.net_output_bytes_total);
        const netInRate = computeRates(ts, netIn);
        const netOutRate = computeRates(ts, netOut);

        // Evictions/s (rate from counter)
        const evictions = history.map(h => h.eviction_keys_total);
        const evictionRate = computeRates(ts, evictions);

        // CPU (rates from counters — fraction of one core)
        const cpuUser = history.map(h => h.cpu_user_seconds);
        const cpuSystem = history.map(h => h.cpu_system_seconds);
        const cpuUserPct = computeRates(ts, cpuUser).map(v => v == null ? null : v * 100);
        const cpuSystemPct = computeRates(ts, cpuSystem).map(v => v == null ? null : v * 100);

        // Hit Rate % (instantaneous ratio)
        const hits = history.map(h => h.keyspace_hits_total);
        const misses = history.map(h => h.keyspace_misses_total);
        const hitRate = hits.map((h, i) => {
            const total = h + misses[i];
            return total > 0 ? (h / total) * 100 : null;
        });

        // Command Errors/s (rate from counter)
        const cmdErrors = history.map(h => h.commands_errors_total);
        const cmdErrorRate = computeRates(ts, cmdErrors);

        // WAL Writes/s (rate from counter)
        const walWrites = history.map(h => h.wal_writes_total);
        const walWriteRate = computeRates(ts, walWrites);

        // Blocked Clients (gauge)
        const blockedClients = history.map(h => h.blocked_clients);

        return {
            ts, cmdRate, memUsed, memRss, memMax, conns, keys,
            netInRate, netOutRate, evictionRate,
            cpuUserPct, cpuSystemPct, hitRate, cmdErrorRate, walWriteRate, blockedClients,
        };
    }

    function getChartWidth() {
        const container = document.querySelector('.chart-area');
        return container ? container.clientWidth : 400;
    }

    function createCharts() {
        const w = getChartWidth();
        const h = 200;

        const emptyTs = [Date.now() / 1000];
        const emptyVal = [0];

        // Commands/s
        const cmdEl = document.getElementById('chart-commands-area');
        if (cmdEl) {
            const opts = baseOpts('Commands/s', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtRate(v));
            opts.series = [
                { value: fmtTime },
                series('cmd/s', COLORS.green, { fill: 'rgba(107,170,61,0.1)', value: (u, v) => v == null ? '—' : fmtRate(v) }),
            ];
            charts.commands = new uPlot(opts, [emptyTs, emptyVal], cmdEl);
        }

        // Memory
        const memEl = document.getElementById('chart-memory-area');
        if (memEl) {
            const opts = baseOpts('Memory', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtBytes(v));
            opts.series = [
                { value: fmtTime },
                series('Used', COLORS.blue, { fill: 'rgba(88,166,255,0.1)', value: (u, v) => v == null ? '—' : fmtBytes(v) }),
                series('RSS', COLORS.orange, { value: (u, v) => v == null ? '—' : fmtBytes(v) }),
                series('Max', COLORS.red, { width: 1, dash: [5, 5], value: (u, v) => v == null ? '—' : fmtBytes(v) }),
            ];
            charts.memory = new uPlot(opts, [emptyTs, emptyVal, emptyVal, emptyVal], memEl);
        }

        // Connections
        const connEl = document.getElementById('chart-connections-area');
        if (connEl) {
            const opts = baseOpts('Connections', w, h);
            opts.series = [
                { value: fmtTime },
                series('Current', COLORS.lime, { fill: 'rgba(141,198,63,0.1)', value: (u, v) => v == null ? '—' : v.toFixed(0) }),
            ];
            charts.connections = new uPlot(opts, [emptyTs, emptyVal], connEl);
        }

        // Key Count
        const keysEl = document.getElementById('chart-keys-area');
        if (keysEl) {
            const opts = baseOpts('Key Count', w, h);
            opts.series = [
                { value: fmtTime },
                series('Total', COLORS.purple, { fill: 'rgba(188,140,255,0.1)', value: (u, v) => v == null ? '—' : v.toFixed(0) }),
            ];
            charts.keys = new uPlot(opts, [emptyTs, emptyVal], keysEl);
        }

        // Network I/O
        const netEl = document.getElementById('chart-network-area');
        if (netEl) {
            const opts = baseOpts('Network', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtBytes(v) + '/s');
            opts.series = [
                { value: fmtTime },
                series('In', COLORS.cyan, { value: (u, v) => v == null ? '—' : fmtBytes(v) + '/s' }),
                series('Out', COLORS.yellow, { value: (u, v) => v == null ? '—' : fmtBytes(v) + '/s' }),
            ];
            charts.network = new uPlot(opts, [emptyTs, emptyVal, emptyVal], netEl);
        }

        // Evictions/s
        const evEl = document.getElementById('chart-evictions-area');
        if (evEl) {
            const opts = baseOpts('Evictions/s', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtIntRate(v));
            opts.series = [
                { value: fmtTime },
                series('evict/s', COLORS.red, { fill: 'rgba(248,81,73,0.1)', value: (u, v) => v == null ? '—' : fmtIntRate(v) }),
            ];
            charts.evictions = new uPlot(opts, [emptyTs, emptyVal], evEl);
        }

        // CPU
        const cpuEl = document.getElementById('chart-cpu-area');
        if (cpuEl) {
            const opts = baseOpts('CPU', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtPct(v));
            opts.series = [
                { value: fmtTime },
                series('User', COLORS.blue, { fill: 'rgba(88,166,255,0.1)', value: (u, v) => v == null ? '—' : fmtPct(v) }),
                series('System', COLORS.orange, { value: (u, v) => v == null ? '—' : fmtPct(v) }),
            ];
            charts.cpu = new uPlot(opts, [emptyTs, emptyVal, emptyVal], cpuEl);
        }

        // Hit Rate
        const hitEl = document.getElementById('chart-hitrate-area');
        if (hitEl) {
            const opts = baseOpts('Hit Rate', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtPct(v));
            opts.series = [
                { value: fmtTime },
                series('Hit %', COLORS.green, { fill: 'rgba(107,170,61,0.1)', value: (u, v) => v == null ? '—' : fmtPct(v) }),
            ];
            charts.hitrate = new uPlot(opts, [emptyTs, emptyVal], hitEl);
        }

        // Command Errors/s
        const errEl = document.getElementById('chart-errors-area');
        if (errEl) {
            const opts = baseOpts('Command Errors/s', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtIntRate(v));
            opts.series = [
                { value: fmtTime },
                series('err/s', COLORS.red, { fill: 'rgba(248,81,73,0.1)', value: (u, v) => v == null ? '—' : fmtIntRate(v) }),
            ];
            charts.errors = new uPlot(opts, [emptyTs, emptyVal], errEl);
        }

        // WAL Writes/s
        const walEl = document.getElementById('chart-wal-area');
        if (walEl) {
            const opts = baseOpts('WAL Writes/s', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtIntRate(v));
            opts.series = [
                { value: fmtTime },
                series('writes/s', COLORS.cyan, { fill: 'rgba(57,211,83,0.1)', value: (u, v) => v == null ? '—' : fmtIntRate(v) }),
            ];
            charts.wal = new uPlot(opts, [emptyTs, emptyVal], walEl);
        }

        // Blocked Clients
        const blkEl = document.getElementById('chart-blocked-area');
        if (blkEl) {
            const opts = baseOpts('Blocked Clients', w, h);
            opts.series = [
                { value: fmtTime },
                series('Blocked', COLORS.yellow, { fill: 'rgba(227,179,65,0.1)', value: (u, v) => v == null ? '—' : v.toFixed(0) }),
            ];
            charts.blocked = new uPlot(opts, [emptyTs, emptyVal], blkEl);
        }
    }

    function updateCharts() {
        if (history.length < 2) return;

        const d = buildData();

        if (charts.commands) charts.commands.setData([d.ts, d.cmdRate]);
        if (charts.memory) charts.memory.setData([d.ts, d.memUsed, d.memRss, d.memMax]);
        if (charts.connections) charts.connections.setData([d.ts, d.conns]);
        if (charts.keys) charts.keys.setData([d.ts, d.keys]);
        if (charts.network) charts.network.setData([d.ts, d.netInRate, d.netOutRate]);
        if (charts.evictions) charts.evictions.setData([d.ts, d.evictionRate]);
        if (charts.cpu) charts.cpu.setData([d.ts, d.cpuUserPct, d.cpuSystemPct]);
        if (charts.hitrate) charts.hitrate.setData([d.ts, d.hitRate]);
        if (charts.errors) charts.errors.setData([d.ts, d.cmdErrorRate]);
        if (charts.wal) charts.wal.setData([d.ts, d.walWriteRate]);
        if (charts.blocked) charts.blocked.setData([d.ts, d.blockedClients]);
    }

    async function poll() {
        try {
            const resp = await fetch('/debug/api/metrics');
            if (!resp.ok) return;
            const snapshot = await resp.json();

            history.push(snapshot);
            if (history.length > MAX_POINTS) {
                history.shift();
            }

            if (!initialized) {
                createCharts();
                initialized = true;
            }

            updateCharts();
        } catch (e) {
            // Silently ignore fetch errors
        }
    }

    // Handle window resize
    let resizeTimer = null;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => {
            const w = getChartWidth();
            Object.values(charts).forEach(c => c.setSize({ width: w, height: 200 }));
        }, 200);
    });

    // Public API
    window.FrogDBCharts = {
        start() {
            if (pollTimer) return;
            const check = () => {
                if (document.getElementById('chart-commands-area')) {
                    poll();
                    pollTimer = setInterval(poll, POLL_INTERVAL);
                } else {
                    setTimeout(check, 100);
                }
            };
            check();
        },
        stop() {
            if (pollTimer) {
                clearInterval(pollTimer);
                pollTimer = null;
            }
        },
        toggleUTC(enabled) {
            useUTC = enabled;
        },
    };
})();
