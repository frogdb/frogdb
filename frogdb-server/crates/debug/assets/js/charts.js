// FrogDB Debug Dashboard - Chart Management
// Uses uPlot for time-series visualization with client-side rate computation.

(function() {
    'use strict';

    const MAX_POINTS = 300; // 10 min at 2s interval
    const POLL_INTERVAL = 2000; // 2 seconds

    // Dark theme colors matching style.css
    const COLORS = {
        teal:   '#53a8b6',
        orange: '#ff9800',
        blue:   '#2196f3',
        purple: '#ab47bc',
        green:  '#4caf50',
        red:    '#ef5350',
        cyan:   '#00bcd4',
        yellow: '#ffeb3b',
    };

    const GRID_COLOR = 'rgba(42, 58, 94, 0.6)';
    const AXIS_COLOR = '#a0a0b0';
    const BG_COLOR = '#16213e';

    // Rolling data buffer
    let history = [];
    let charts = {};
    let pollTimer = null;
    let initialized = false;

    // Base uPlot options shared by all charts
    function baseOpts(title, width, height) {
        return {
            width: width,
            height: height,
            cursor: { show: true, drag: { x: false, y: false } },
            legend: { show: true, live: true },
            axes: [
                {
                    stroke: AXIS_COLOR,
                    grid: { stroke: GRID_COLOR, width: 1 },
                    ticks: { stroke: GRID_COLOR, width: 1 },
                    values: (u, vals) => vals.map(v => {
                        const d = new Date(v * 1000);
                        return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                    }),
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

    // Format rate (per second)
    function fmtRate(val) {
        if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M/s';
        if (val >= 1000) return (val / 1000).toFixed(1) + 'K/s';
        return val.toFixed(1) + '/s';
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
        const memMax = history.map(h => h.memory_max_bytes);

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

        return { ts, cmdRate, memUsed, memRss, memMax, conns, keys, netInRate, netOutRate, evictionRate };
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
                {},
                { label: 'cmd/s', stroke: COLORS.teal, width: 2, fill: 'rgba(83,168,182,0.1)' },
            ];
            charts.commands = new uPlot(opts, [emptyTs, emptyVal], cmdEl);
        }

        // Memory
        const memEl = document.getElementById('chart-memory-area');
        if (memEl) {
            const opts = baseOpts('Memory', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtBytes(v));
            opts.series = [
                {},
                { label: 'Used', stroke: COLORS.blue, width: 2, fill: 'rgba(33,150,243,0.1)' },
                { label: 'RSS', stroke: COLORS.orange, width: 2 },
                { label: 'Max', stroke: COLORS.red, width: 1, dash: [5, 5] },
            ];
            charts.memory = new uPlot(opts, [emptyTs, emptyVal, emptyVal, emptyVal], memEl);
        }

        // Connections
        const connEl = document.getElementById('chart-connections-area');
        if (connEl) {
            const opts = baseOpts('Connections', w, h);
            opts.series = [
                {},
                { label: 'Current', stroke: COLORS.green, width: 2, fill: 'rgba(76,175,80,0.1)' },
            ];
            charts.connections = new uPlot(opts, [emptyTs, emptyVal], connEl);
        }

        // Keys
        const keysEl = document.getElementById('chart-keys-area');
        if (keysEl) {
            const opts = baseOpts('Keys', w, h);
            opts.series = [
                {},
                { label: 'Total', stroke: COLORS.purple, width: 2, fill: 'rgba(171,71,188,0.1)' },
            ];
            charts.keys = new uPlot(opts, [emptyTs, emptyVal], keysEl);
        }

        // Network I/O
        const netEl = document.getElementById('chart-network-area');
        if (netEl) {
            const opts = baseOpts('Network', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtBytes(v) + '/s');
            opts.series = [
                {},
                { label: 'In', stroke: COLORS.cyan, width: 2 },
                { label: 'Out', stroke: COLORS.yellow, width: 2 },
            ];
            charts.network = new uPlot(opts, [emptyTs, emptyVal, emptyVal], netEl);
        }

        // Evictions/s
        const evEl = document.getElementById('chart-evictions-area');
        if (evEl) {
            const opts = baseOpts('Evictions/s', w, h);
            opts.axes[1].values = (u, vals) => vals.map(v => fmtRate(v));
            opts.series = [
                {},
                { label: 'evict/s', stroke: COLORS.red, width: 2, fill: 'rgba(239,83,80,0.1)' },
            ];
            charts.evictions = new uPlot(opts, [emptyTs, emptyVal], evEl);
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
            // If charts haven't been created yet, wait for the partial to load
            const check = () => {
                if (document.getElementById('chart-commands-area')) {
                    poll(); // Initial poll
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
    };
})();
